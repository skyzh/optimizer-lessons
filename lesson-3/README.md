# A Memo Merge Is a Transaction

Lesson 1 made expression lookup canonical. Lesson 2 made group merging converge:
when a child group changes, its parents are re-keyed, duplicate expressions are
collapsed, and newly equivalent parent groups join the same worklist. This lesson
keeps that implementation and asks a different question: **in what order must all
of those changes happen?**

The answer matters because one equivalence fact is stored in several places at
once. A merge is complete only when every one of them names the same surviving
identity.

## Start with one small memo

The executable fixture contains two scan groups and two matching projections:

| Group | Expression | Extra state |
| --- | --- | --- |
| `G0` | `E0 = Scan("a")` | — |
| `G1` | `E1 = Scan("b")` | — |
| `G2` | `E2 = Project("x", G0)` | winner `E2@10` |
| `G3` | `E3 = Project("x", G1)` | winner `E3@2` |

Two outer projections, `E4 = Project("y", G2)` and
`E5 = Project("y", G3)`, make the collision cascade visible. A simulated
optimizer task still holds the old pair `G3/E3` when the merge begins.

Suppose a rule proves that `G1` is equivalent to `G0`. Replacing `G1` with `G0`
makes `E3` and `E2` the same canonical expression. Their owner groups must
therefore merge, which in turn makes `E5` and `E4` identical. The final state
should be:

```text
G1 -> G0
E3 -> E2
G3 -> G2
G5 -> G4
old task G3/E3 -> G2/E2
winner of G2 = E2@2
```

That result spans several identity-bearing planes:

| Plane | What must be true after the merge |
| --- | --- |
| expression ownership | `E1` belongs to the surviving `G0` |
| group redirects | every old group ID reaches one live representative |
| stored children | no live expression still stores a redirected child |
| canonical keys | one canonical expression has one live expression ID |
| expression redirects | the removed `E3` handle reaches `E2` |
| parent reachability | both projection levels converge to a fixed point |
| winner and task state | stale handles are reduced and the cheaper winner survives |

## Predict the three failures

Before running the program, predict the first broken plane in each variant:

1. **Redirect first.** Install `G1 -> G0`, then try to retrieve and migrate the
   expressions owned by `G1`.
2. **Skip re-key and cascade.** Move `E1` and install the group redirect, but do
   not rewrite expressions whose child was `G1`.
3. **Skip handle and winner repair.** Complete the structural merge, but write
   through the task's old `G3/E3` pair and always retain the destination winner.

The important question is not merely whether each version fails. Ask what the
step changed about the meaning of a lookup:

- After the redirect, does reading `G1` still reach the source row that must move?
- After a child replacement, is the expression still stored under its old key?
- After duplicate expressions collapse, can a task safely write through the ID it
  captured before the merge?

## The canonical transaction

The repair captures source-owned expressions and parents while `G1` still means
the source group. It then performs the coordinated migration:

1. migrate `E1` from `G1` to `G0`;
2. install `G1 -> G0`;
3. rewrite `E3` from `Project("x", G1)` to `Project("x", G0)`;
4. collapse the collision by redirecting `E3 -> E2` and `G3 -> G2`;
5. repeat the re-key and collision repair for `E5`, reaching `G5 -> G4`;
6. reduce the task's old `G3/E3` handles to `G2/E2`; and
7. keep the cheaper full winner, `E2@2`, in the surviving group.

The sequence is a fixed-point transaction. A re-key collision is not permission
to overwrite one hash entry with another. It is new evidence that two owner
groups are equivalent, so it adds another merge to the worklist.

## Run the replay

From the repository root, run:

```console
cargo run -p optimizer-blog-lesson-3
```

The output is deterministic:

```text
scenario=redirect-first
  redirect G1->G0
  capture G1 exprs=[E1] parents=[E3]
  skip source migration after redirect
  failure E1 stayed in redirected G1 instead of G0
  failure E3 still uses G1 instead of G0
  failure E2 and E3 have the same canonical key
  failure G4 and G5 did not converge

scenario=skip-rekey-and-cascade
  capture G1 exprs=[E1] parents=[E3]
  migrate E1 G1->G0
  redirect G1->G0
  skip parent re-key and collision cascade
  failure E3 still uses G1 instead of G0
  failure E2 and E3 have the same canonical key
  failure G4 and G5 did not converge

scenario=skip-handle-and-winner-repair
  capture G1 exprs=[E1] parents=[E3]
  migrate E1 G1->G0
  redirect G1->G0
  rekey E3 G1->G0
  redirect E3->E2
  redirect G3->G2
  rekey E5 G3->G2
  redirect E5->E4
  redirect G5->G4
  skip task-handle reduction
  winner G3=E3@2
  failure task wrote through stale G3/E3
  failure winner cost is 10, expected 2

scenario=canonical
  capture G1 exprs=[E1] parents=[E3]
  migrate E1 G1->G0
  redirect G1->G0
  rekey E3 G1->G0
  redirect E3->E2
  redirect G3->G2
  rekey E5 G3->G2
  redirect E5->E4
  redirect G5->G4
  resolve task G3/E3->G2/E2
  winner G2=E2@2
  final G1->G0 E3->E2 parents=G5->G4 task=G2/E2 winner=E2@2
```

The first two variants show why redirects and structural repair must be ordered.
The third shows that a structurally canonical memo can still be wrong if work and
cost state retain obsolete identities. Only the canonical replay makes every
plane agree.

Run the focused checks with:

```console
cargo test -p optimizer-blog-lesson-3
```

The eight tests independently check migration order, recursive convergence,
unique canonical ownership, stale-handle reduction, cheaper-winner retention,
full-scan/backlink agreement, parity with both Lesson 2 merge paths, and stable
console output.

## What this model establishes

The failure order is grounded in the history of `optd-original`. A
[redirect-before-migration bug](https://github.com/cmu-db/optd-original/pull/114)
lost source expressions. The later
[memo rewrite](https://github.com/cmu-db/optd-original/pull/203) made expression
ownership, redirects, canonical keys, and cascading parent repair explicit.
Separate fixes made
[state writes reduce old group handles](https://github.com/cmu-db/optd-original/commit/6497fe7fc03254a6a0d7eed25d3a9cbbfa047508)
and made merges
[retain the cheaper winner](https://github.com/cmu-db/optd-original/pull/272).
Current `optd-original` discovers affected parents with a full scan; Lesson 2's
backlinks are an alternative index for the same parent relation, not a correction
to current production code.

Here, “transaction” means one coordinated in-memory mutation whose identity
planes agree again at the boundary. The replay does not model concurrency,
isolation, rollback, durability, recursive CTEs, statistics or property
invalidation, or winners keyed by required physical properties. It reconstructs
the smallest causal sequence supported by the documented failures; it is not a
claim that every historical scheduler state can still be executed on today's
production structs.

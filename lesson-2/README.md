# A Good Memo Table Is Hard to Build

This lesson continues from the memo table introduced in Lesson 1. The examples
are split into the bugs discovered while implementing group merging:

1. `s01_shallow_merge.rs`: moving expressions is not enough;
2. `s02_rewrite_children.rs`: rewriting child group IDs can create duplicates;
3. `s03_cascading_merge.rs`: duplicate expressions require cascading group merges;
4. `s04_parent_backlinks.rs`: parent backlinks avoid a full memo scan on each merge.

The correctness-first implementation in step 3 scans all memo expressions to
find references to the group being merged. Step 4 maintains the inverse edge,
`child group -> parent expressions`, so each repair starts from only the affected
parents. Both steps use an explicit worklist to reach the collision fixed point
without growing the call stack. Collisions can still trigger more merges and
eventually touch a large part of the memo, but unrelated expressions are no
longer inspected by every individual merge.

Run the examples with:

```console
cargo test -p optimizer-blog-lesson-2
```

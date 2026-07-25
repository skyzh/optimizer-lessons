# A Good Memo Table Is Hard to Build

This lesson continues from the memo table introduced in Lesson 1. The examples
are split into the bugs discovered while implementing group merging:

1. `s01_shallow_merge.rs`: moving expressions is not enough;
2. `s02_rewrite_children.rs`: rewriting child group IDs can create duplicates;
3. `s03_cascading_merge.rs`: duplicate expressions require recursive group merges;
4. `s04_stable_handles.rs`: callers may still hold IDs invalidated by a merge.

Run the examples with:

```console
cargo test -p optimizer-blog-lesson-2
```

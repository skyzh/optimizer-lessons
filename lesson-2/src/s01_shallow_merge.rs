use crate::{GroupId, Memo};

impl Memo {
    /// The tempting implementation: move the expressions and remember that the
    /// old group ID now means the new one.
    pub fn merge_group_shallow(&mut self, merge_into: GroupId, merge_from: GroupId) -> GroupId {
        self.move_group(merge_into, merge_from)
    }
}

#[cfg(test)]
mod tests {
    use crate::{GroupId, Memo, MemoExpr, RelNodeType};

    fn unary(typ: RelNodeType, child: GroupId) -> MemoExpr {
        MemoExpr::new(typ, vec![child])
    }

    #[test]
    fn shallow_merge_leaves_stale_children() {
        let mut memo = Memo::new();
        let (scan, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1"), vec![]));
        let (project_1, _) = memo.add_expr(unary(RelNodeType::Project("x"), scan));
        let (project_2, project_2_expr) =
            memo.add_expr(unary(RelNodeType::Project("x"), project_1));
        let (filter_group, _) = memo.add_expr(unary(RelNodeType::Filter("x > 1"), project_1));

        memo.merge_group_shallow(project_2, project_1);

        // !project_1 is gone, but old expressions still contain it.
        assert_eq!(memo.expr(project_2_expr).children, vec![project_1]);
        assert_eq!(memo.representative(project_1), project_2);

        // New expressions use representative IDs. They no longer match the
        // stale expressions in the reverse index, so duplicate groups appear.
        let (duplicate_project, _) = memo.add_expr(unary(RelNodeType::Project("x"), project_2));
        let (duplicate_filter, _) = memo.add_expr(unary(RelNodeType::Filter("x > 1"), project_2));
        assert_ne!(duplicate_project, project_2);
        assert_ne!(duplicate_filter, filter_group);
        assert!(memo.check_invariants().is_err());
    }
}

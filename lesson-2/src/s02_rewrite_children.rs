use crate::{ExprId, GroupId, Memo};

impl Memo {
    /// Fix stale child IDs by scanning every expression, but deliberately do
    /// not resolve expression collisions yet.
    pub fn merge_group_rewrite_only(
        &mut self,
        merge_into: GroupId,
        merge_from: GroupId,
    ) -> GroupId {
        let merge_into = self.representative(merge_into);
        let merge_from = self.representative(merge_from);
        if merge_into == merge_from {
            return merge_into;
        }
        self.move_group(merge_into, merge_from);

        let mut affected = self
            .exprs
            .iter()
            .filter_map(|(expr_id, expr)| expr.children.contains(&merge_from).then_some(*expr_id))
            .collect::<Vec<ExprId>>();
        affected.sort();

        for expr_id in affected {
            let old_expr = self.exprs[&expr_id].clone();
            let new_expr = self.canonicalize_expr(old_expr.clone());
            if self.expr_to_id.get(&old_expr) == Some(&expr_id) {
                self.expr_to_id.remove(&old_expr);
            }
            self.remove_parent_links(expr_id, &old_expr);
            self.exprs.insert(expr_id, new_expr.clone());
            // This can overwrite another expression with the same key.
            self.expr_to_id.insert(new_expr.clone(), expr_id);
            self.add_parent_links(expr_id, &new_expr);
        }
        merge_into
    }
}

#[cfg(test)]
mod tests {
    use crate::{Memo, MemoExpr, RelNodeType};

    #[test]
    fn rewriting_children_can_create_duplicate_expressions() {
        let mut memo = Memo::new();
        let (scan_1, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1"), vec![]));
        let (scan_2, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1-alias"), vec![]));
        let project_1 = MemoExpr::new(RelNodeType::Project("x"), vec![scan_1]);
        let project_2 = MemoExpr::new(RelNodeType::Project("x"), vec![scan_2]);
        let (project_group_1, _) = memo.add_expr(project_1.clone());
        let (project_group_2, _) = memo.add_expr(project_2);

        memo.merge_group_rewrite_only(scan_1, scan_2);

        // Project(!scan_2) became Project(!scan_1). The two expressions are
        // identical, so their parent groups are equivalent too.
        assert_eq!(memo.groups_containing(&project_1).len(), 2);
        assert_ne!(
            memo.representative(project_group_1),
            memo.representative(project_group_2)
        );
        assert!(memo.check_invariants().is_err());
    }
}

use crate::{ExprId, GroupId, Memo};

impl Memo {
    /// Merge two equivalent groups while preserving all memo invariants. This
    /// correctness-first version scans the whole memo to find parent expressions.
    pub fn merge_group_scanning(&mut self, merge_into: GroupId, merge_from: GroupId) -> GroupId {
        let result = self.merge_group_scanning_inner(merge_into, merge_from);
        debug_assert!(self.check_invariants().is_ok());
        result
    }

    fn merge_group_scanning_inner(&mut self, merge_into: GroupId, merge_from: GroupId) -> GroupId {
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

        let mut pending_group_merges = Vec::new();
        for expr_id in affected {
            // An earlier collision in this pass may already have removed it.
            let Some(old_expr) = self.exprs.get(&expr_id).cloned() else {
                continue;
            };
            let new_expr = self.canonicalize_expr(old_expr.clone());
            if old_expr == new_expr {
                continue;
            }
            if self.expr_to_id.get(&old_expr) == Some(&expr_id) {
                self.expr_to_id.remove(&old_expr);
            }
            self.remove_parent_links(expr_id, &old_expr);

            if let Some(&existing_expr_id) = self.expr_to_id.get(&new_expr) {
                let existing_expr_id = self.representative_expr(existing_expr_id);
                let duplicate_group = self.expr_to_group.remove(&expr_id).unwrap();
                let existing_group = self.group_of_expr(existing_expr_id);

                self.exprs.remove(&expr_id);
                self.groups
                    .get_mut(&duplicate_group)
                    .unwrap()
                    .exprs
                    .remove(&expr_id);
                self.redirect_expr(expr_id, existing_expr_id);

                if self.representative(duplicate_group) != existing_group {
                    pending_group_merges.push((existing_group, duplicate_group));
                }
            } else {
                self.exprs.insert(expr_id, new_expr.clone());
                self.expr_to_id.insert(new_expr.clone(), expr_id);
                self.add_parent_links(expr_id, &new_expr);
            }
        }

        // A rewritten expression can collide with an existing expression in a
        // different group. Those groups are equivalent, and merging them can
        // cause the same situation in their parents.
        for (merge_into, merge_from) in pending_group_merges {
            let merge_into = self.representative(merge_into);
            let merge_from = self.representative(merge_from);
            if merge_into != merge_from {
                self.merge_group_scanning_inner(merge_into, merge_from);
            }
        }

        self.representative(merge_into)
    }
}

#[cfg(test)]
mod tests {
    use crate::{GroupId, Memo, MemoExpr, RelNodeType};

    fn unary(typ: RelNodeType, child: GroupId) -> MemoExpr {
        MemoExpr::new(typ, vec![child])
    }

    #[test]
    fn merge_rewrites_self_and_external_references() {
        let mut memo = Memo::new();
        let (scan, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1"), vec![]));
        let (project_1, _) = memo.add_expr(unary(RelNodeType::Project("x"), scan));
        let (project_2, _) = memo.add_expr(unary(RelNodeType::Project("x"), project_1));
        let (filter_group, _) = memo.add_expr(unary(RelNodeType::Filter("x > 1"), project_1));

        let merged = memo.merge_group_scanning(project_2, project_1);
        assert_eq!(merged, project_2);

        let (same_project, _) = memo.add_expr(unary(RelNodeType::Project("x"), project_2));
        let (same_filter, _) = memo.add_expr(unary(RelNodeType::Filter("x > 1"), project_2));
        assert_eq!(same_project, project_2);
        assert_eq!(same_filter, filter_group);
        assert!(memo.check_invariants().is_ok());
    }

    #[test]
    fn expression_collisions_cascade_to_parent_groups() {
        let mut memo = Memo::new();
        let (scan_1, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1"), vec![]));
        let (scan_2, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1-alias"), vec![]));
        let (project_1, _) = memo.add_expr(unary(RelNodeType::Project("x"), scan_1));
        let (project_2, _) = memo.add_expr(unary(RelNodeType::Project("x"), scan_2));
        let (outer_1, _) = memo.add_expr(unary(RelNodeType::Project("y"), project_1));
        let (outer_2, _) = memo.add_expr(unary(RelNodeType::Project("y"), project_2));

        memo.merge_group_scanning(scan_1, scan_2);

        assert_eq!(
            memo.representative(project_1),
            memo.representative(project_2)
        );
        assert_eq!(memo.representative(outer_1), memo.representative(outer_2));
        assert_eq!(memo.group_count(), 3);
        assert_eq!(memo.expression_count(), 4);
        assert!(memo.check_invariants().is_ok());
    }

    #[test]
    fn one_merge_can_create_multiple_independent_collisions() {
        let mut memo = Memo::new();
        let (scan_1, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1"), vec![]));
        let (scan_2, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1-alias"), vec![]));
        let (project_1, _) = memo.add_expr(unary(RelNodeType::Project("x"), scan_1));
        let (filter_1, _) = memo.add_expr(unary(RelNodeType::Filter("x > 1"), scan_1));
        let (project_2, _) = memo.add_expr(unary(RelNodeType::Project("x"), scan_2));
        let (filter_2, _) = memo.add_expr(unary(RelNodeType::Filter("x > 1"), scan_2));

        memo.merge_group_scanning(scan_1, scan_2);

        assert_eq!(
            memo.representative(project_1),
            memo.representative(project_2)
        );
        assert_eq!(memo.representative(filter_1), memo.representative(filter_2));
        assert_eq!(memo.group_count(), 3);
        assert_eq!(memo.expression_count(), 4);
        assert!(memo.check_invariants().is_ok());
    }
}

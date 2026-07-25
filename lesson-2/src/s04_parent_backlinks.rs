use crate::{GroupId, Memo};

impl Memo {
    /// Merge equivalent groups by following the losing group's parent-expression
    /// backlinks instead of scanning every expression in the memo.
    pub fn merge_group(&mut self, merge_into: GroupId, merge_from: GroupId) -> GroupId {
        let result = self.merge_group_with_backlinks_inner(merge_into, merge_from);
        debug_assert!(self.check_invariants().is_ok());
        result
    }

    fn merge_group_with_backlinks_inner(
        &mut self,
        merge_into: GroupId,
        merge_from: GroupId,
    ) -> GroupId {
        let merge_into = self.representative(merge_into);
        let merge_from = self.representative(merge_from);
        if merge_into == merge_from {
            return merge_into;
        }

        // Capture the inverse edge before redirecting the group. These are the
        // only expressions whose hash keys can change because of this merge.
        let affected = self
            .parent_exprs
            .get(&merge_from)
            .cloned()
            .unwrap_or_default();
        self.move_group(merge_into, merge_from);

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

        // Repairing a parent key can reveal another duplicate. Its owner groups
        // are equivalent, so follow their backlinks and continue to a fixed point.
        for (merge_into, merge_from) in pending_group_merges {
            let merge_into = self.representative(merge_into);
            let merge_from = self.representative(merge_from);
            if merge_into != merge_from {
                self.merge_group_with_backlinks_inner(merge_into, merge_from);
            }
        }

        self.representative(merge_into)
    }
}

#[cfg(test)]
mod tests {
    use crate::{ExprId, GroupId, Memo, MemoExpr, RelNodeType};

    fn unary(typ: RelNodeType, child: GroupId) -> MemoExpr {
        MemoExpr::new(typ, vec![child])
    }

    #[test]
    fn backlinks_track_direct_parent_expressions() {
        let mut memo = Memo::new();
        let (scan, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1"), vec![]));
        let (_, project_expr) = memo.add_expr(unary(RelNodeType::Project("x"), scan));
        let (_, filter_expr) = memo.add_expr(unary(RelNodeType::Filter("x > 1"), scan));

        assert_eq!(memo.parent_exprs(scan), vec![project_expr, filter_expr]);

        // One expression is one parent even when it uses the group twice.
        let (_, repeated_child_expr) = memo.add_expr(MemoExpr::new(
            RelNodeType::Filter("x = x"),
            vec![scan, scan],
        ));
        assert_eq!(
            memo.parent_exprs(scan),
            vec![project_expr, filter_expr, repeated_child_expr]
        );
        assert!(memo.check_invariants().is_ok());
    }

    #[test]
    fn backlinks_are_repaired_through_cascading_merges() {
        let mut memo = Memo::new();
        let (scan_1, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1"), vec![]));
        let (scan_2, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1-alias"), vec![]));
        let (project_1, project_expr_1) = memo.add_expr(unary(RelNodeType::Project("x"), scan_1));
        let (project_2, project_expr_2) = memo.add_expr(unary(RelNodeType::Project("x"), scan_2));
        let (outer_1, outer_expr_1) = memo.add_expr(unary(RelNodeType::Project("y"), project_1));
        let (outer_2, _) = memo.add_expr(unary(RelNodeType::Project("y"), project_2));

        assert_eq!(memo.parent_exprs(scan_2), vec![project_expr_2]);
        memo.merge_group(scan_1, scan_2);

        assert_eq!(
            memo.representative(project_1),
            memo.representative(project_2)
        );
        assert_eq!(memo.representative(outer_1), memo.representative(outer_2));
        assert_eq!(memo.parent_exprs(scan_1), vec![project_expr_1]);
        assert_eq!(memo.parent_exprs(project_1), vec![outer_expr_1]);
        assert_eq!(memo.parent_exprs(outer_1), Vec::<ExprId>::new());
        assert!(memo.check_invariants().is_ok());
    }
}

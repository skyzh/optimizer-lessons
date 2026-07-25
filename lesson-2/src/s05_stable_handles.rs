#[cfg(test)]
mod tests {
    use crate::{GroupId, Memo, MemoExpr, RelNodeType, Winner};

    fn unary(typ: RelNodeType, child: GroupId) -> MemoExpr {
        MemoExpr::new(typ, vec![child])
    }

    #[test]
    fn stale_group_and_expression_ids_remain_usable() {
        let mut memo = Memo::new();
        let (scan_1, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1"), vec![]));
        let (scan_2, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1-alias"), vec![]));
        let (_, project_expr_1) = memo.add_expr(unary(RelNodeType::Project("x"), scan_1));
        let (project_group_2, project_expr_2) =
            memo.add_expr(unary(RelNodeType::Project("x"), scan_2));

        memo.merge_group(scan_1, scan_2);

        assert_eq!(memo.representative_expr(project_expr_2), project_expr_1);
        assert_eq!(memo.expr(project_expr_2), memo.expr(project_expr_1));
        assert_eq!(
            memo.group_of_expr(project_expr_2),
            memo.group_of_expr(project_expr_1)
        );

        // The optimizer may finish a task created before the merge. Both of
        // its old handles are redirected before the winner is installed.
        memo.set_winner(project_group_2, project_expr_2, 10);
        assert_eq!(
            memo.winner(project_group_2),
            Some(Winner {
                expr_id: project_expr_1,
                cost: 10,
            })
        );
        assert!(memo.check_invariants().is_ok());
    }

    #[test]
    fn merging_groups_keeps_the_cheaper_winner() {
        let mut memo = Memo::new();
        let (scan_1, expr_1) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1"), vec![]));
        let (scan_2, expr_2) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t2"), vec![]));
        memo.set_winner(scan_1, expr_1, 100);
        memo.set_winner(scan_2, expr_2, 10);

        let merged = memo.merge_group(scan_1, scan_2);

        assert_eq!(
            memo.winner(merged),
            Some(Winner {
                expr_id: expr_2,
                cost: 10,
            })
        );
        assert!(memo.check_invariants().is_ok());
    }
}

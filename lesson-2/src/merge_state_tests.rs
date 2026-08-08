use crate::{GroupId, Memo, MemoExpr, RelNodeType, Winner};

type MergeGroups = fn(&mut Memo, GroupId, GroupId) -> GroupId;

const MERGE_IMPLEMENTATIONS: [(&str, MergeGroups); 2] = [
    ("scanning", Memo::merge_group_scanning),
    ("backlinks", Memo::merge_group),
];

fn unary(typ: RelNodeType, child: GroupId) -> MemoExpr {
    MemoExpr::new(typ, vec![child])
}

#[test]
fn stale_group_and_expression_ids_remain_usable() {
    let mut memo = Memo::new();
    let (scan_1, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1"), vec![]));
    let (scan_2, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1-alias"), vec![]));
    let (_, project_expr_1) = memo.add_expr(unary(RelNodeType::Project("x"), scan_1));
    let (project_group_2, project_expr_2) = memo.add_expr(unary(RelNodeType::Project("x"), scan_2));

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

#[test]
fn deep_unary_parent_cascade_uses_bounded_stack() {
    const DEPTH: usize = 7_000;

    for (implementation, merge_groups) in MERGE_IMPLEMENTATIONS {
        let mut memo = Memo::new();
        let (scan_1, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1"), vec![]));
        let (scan_2, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1-alias"), vec![]));
        let mut parent_1 = scan_1;
        let mut parent_2 = scan_2;

        for _ in 0..DEPTH {
            parent_1 = memo.add_expr(unary(RelNodeType::Project("x"), parent_1)).0;
            parent_2 = memo.add_expr(unary(RelNodeType::Project("x"), parent_2)).0;
        }

        merge_groups(&mut memo, scan_1, scan_2);

        assert_eq!(
            memo.representative(parent_1),
            memo.representative(parent_2),
            "{implementation} merge stopped before reaching the root"
        );
        assert_eq!(memo.group_count(), DEPTH + 1, "{implementation}");
        assert_eq!(memo.expression_count(), DEPTH + 2, "{implementation}");
        assert!(memo.check_invariants().is_ok(), "{implementation}");
    }
}

#[test]
fn cascading_merge_handles_cycles_and_repeated_children() {
    for (implementation, merge_groups) in MERGE_IMPLEMENTATIONS {
        let mut memo = Memo::new();
        let (scan_1, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1"), vec![]));
        let (scan_2, _) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("t1-alias"), vec![]));

        memo.add_expr_to_group(
            MemoExpr::new(RelNodeType::Filter("self-cycle"), vec![scan_1]),
            scan_1,
        );
        memo.add_expr_to_group(
            MemoExpr::new(RelNodeType::Filter("self-cycle"), vec![scan_2]),
            scan_2,
        );

        let (repeated_1, _) = memo.add_expr(MemoExpr::new(
            RelNodeType::Filter("x = x"),
            vec![scan_1, scan_1],
        ));
        let (repeated_2, _) = memo.add_expr(MemoExpr::new(
            RelNodeType::Filter("x = x"),
            vec![scan_2, scan_2],
        ));

        merge_groups(&mut memo, scan_1, scan_2);

        assert_eq!(
            memo.representative(repeated_1),
            memo.representative(repeated_2),
            "{implementation} merge missed a repeated-child collision"
        );
        assert!(memo.check_invariants().is_ok(), "{implementation}");
    }
}

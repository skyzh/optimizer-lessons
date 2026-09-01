use optimizer_blog_lesson_2::{Memo, MemoExpr, RelNodeType, Winner};
use optimizer_blog_lesson_3::{
    DESTINATION_OUTER_GROUP, DESTINATION_PROJECT_EXPR, DESTINATION_PROJECT_GROUP,
    DESTINATION_SCAN_GROUP, ExprId, ExprKey, Failure, GroupId, ReplayScenario, SOURCE_OUTER_GROUP,
    SOURCE_PROJECT_EXPR, SOURCE_PROJECT_GROUP, SOURCE_SCAN_GROUP, TaskHandle, initial_snapshot,
    render_demo, replay,
};

fn lesson_2_fixture() -> (Memo, [GroupId; 6], [ExprId; 6]) {
    let mut memo = Memo::new();
    let (scan_1, scan_expr_1) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("a"), vec![]));
    let (scan_2, scan_expr_2) = memo.add_expr(MemoExpr::new(RelNodeType::Scan("b"), vec![]));
    let (project_1, project_expr_1) =
        memo.add_expr(MemoExpr::new(RelNodeType::Project("x"), vec![scan_1]));
    let (project_2, project_expr_2) =
        memo.add_expr(MemoExpr::new(RelNodeType::Project("x"), vec![scan_2]));
    let (outer_1, outer_expr_1) =
        memo.add_expr(MemoExpr::new(RelNodeType::Project("y"), vec![project_1]));
    let (outer_2, outer_expr_2) =
        memo.add_expr(MemoExpr::new(RelNodeType::Project("y"), vec![project_2]));
    memo.set_winner(project_1, project_expr_1, 10);
    memo.set_winner(project_2, project_expr_2, 2);
    (
        memo,
        [scan_1, scan_2, project_1, project_2, outer_1, outer_2],
        [
            scan_expr_1,
            scan_expr_2,
            project_expr_1,
            project_expr_2,
            outer_expr_1,
            outer_expr_2,
        ],
    )
}

#[test]
fn migration_must_happen_before_redirect() {
    let broken = replay(ReplayScenario::RedirectFirst, true);
    assert!(
        broken
            .failures
            .contains(&Failure::SourceExpressionNotMigrated {
                expr_id: ExprId(1),
                from: SOURCE_SCAN_GROUP,
                to: DESTINATION_SCAN_GROUP,
            })
    );

    let fixed = replay(ReplayScenario::Canonical, true);
    assert!(fixed.failures.is_empty());
    assert_eq!(fixed.snapshot.owner_of(ExprId(1)), Some(GroupId(0)));
}

#[test]
fn rekey_and_cascade_reach_the_parent_fixed_point() {
    let broken = replay(ReplayScenario::SkipRekeyAndCascade, true);
    assert!(broken.failures.iter().any(|failure| matches!(
        failure,
        Failure::DuplicateCanonicalExpression {
            first: DESTINATION_PROJECT_EXPR,
            second: SOURCE_PROJECT_EXPR,
        }
    )));
    assert!(
        broken
            .failures
            .contains(&Failure::ParentGroupsDidNotConverge {
                first: DESTINATION_OUTER_GROUP,
                second: SOURCE_OUTER_GROUP,
            })
    );

    let fixed = replay(ReplayScenario::Canonical, true);
    assert_eq!(
        fixed.snapshot.representative_expr(SOURCE_PROJECT_EXPR),
        DESTINATION_PROJECT_EXPR
    );
    assert_eq!(
        fixed.snapshot.representative_group(SOURCE_OUTER_GROUP),
        DESTINATION_OUTER_GROUP
    );
}

#[test]
fn canonical_keys_have_exactly_one_owner() {
    let fixed = replay(ReplayScenario::Canonical, true);
    let project = ExprKey::new("project:x", vec![DESTINATION_SCAN_GROUP]);
    assert_eq!(fixed.snapshot.canonical_owner_count(&project), 1);
    assert!(
        !fixed
            .failures
            .iter()
            .any(|failure| matches!(failure, Failure::DuplicateCanonicalExpression { .. }))
    );
}

#[test]
fn stale_task_handles_are_reduced_before_writes() {
    let broken = replay(ReplayScenario::SkipHandleAndWinnerRepair, true);
    assert!(
        broken
            .failures
            .contains(&Failure::StaleTaskWrite(TaskHandle {
                group_id: SOURCE_PROJECT_GROUP,
                expr_id: SOURCE_PROJECT_EXPR,
            }))
    );

    let fixed = replay(ReplayScenario::Canonical, true);
    assert_eq!(
        fixed.snapshot.resolved_task(),
        Some(TaskHandle {
            group_id: DESTINATION_PROJECT_GROUP,
            expr_id: DESTINATION_PROJECT_EXPR,
        })
    );
}

#[test]
fn merging_winners_retains_the_cheaper_plan() {
    let broken = replay(ReplayScenario::SkipHandleAndWinnerRepair, true);
    assert!(
        broken
            .failures
            .contains(&Failure::ExpensiveWinnerRetained { cost: 10 })
    );

    let fixed = replay(ReplayScenario::Canonical, true);
    assert_eq!(
        fixed.snapshot.winner(DESTINATION_PROJECT_GROUP),
        Some(Winner {
            expr_id: DESTINATION_PROJECT_EXPR,
            cost: 2,
        })
    );
}

#[test]
fn full_scan_and_backlink_discovery_agree() {
    let initial = initial_snapshot(true);
    for id in 0..6 {
        let group_id = GroupId(id);
        assert_eq!(
            initial.full_scan_parents(group_id),
            initial.backlink_parents(group_id),
            "G{id}"
        );
    }

    let fixed = replay(ReplayScenario::Canonical, true);
    assert!(
        !fixed
            .failures
            .iter()
            .any(|failure| matches!(failure, Failure::BacklinkMismatch { .. }))
    );
}

#[test]
fn canonical_replay_matches_both_real_lesson_2_merges() {
    let fixed = replay(ReplayScenario::Canonical, true);
    for merge in [
        Memo::merge_group_scanning as fn(&mut Memo, GroupId, GroupId) -> GroupId,
        Memo::merge_group,
    ] {
        let (mut memo, groups, exprs) = lesson_2_fixture();
        merge(&mut memo, groups[0], groups[1]);

        assert_eq!(memo.representative(groups[1]), groups[0]);
        assert_eq!(memo.representative_expr(exprs[3]), exprs[2]);
        assert_eq!(memo.representative(groups[5]), groups[4]);
        assert_eq!(
            memo.winner(groups[2]),
            Some(Winner {
                expr_id: exprs[2],
                cost: 2,
            })
        );
        assert_eq!(
            fixed.snapshot.representative_group(SOURCE_SCAN_GROUP),
            memo.representative(groups[1])
        );
        assert_eq!(
            fixed.snapshot.representative_expr(SOURCE_PROJECT_EXPR),
            memo.representative_expr(exprs[3])
        );
        assert!(memo.check_invariants().is_ok());
    }
}

#[test]
fn console_output_is_repeatable_and_ends_with_the_canonical_state() {
    let first = render_demo();
    let second = render_demo();
    assert_eq!(first, second);
    assert!(first.contains("scenario=redirect-first"));
    assert!(first.contains("scenario=skip-rekey-and-cascade"));
    assert!(first.contains("scenario=skip-handle-and-winner-repair"));
    assert!(first.contains("scenario=canonical"));
    assert!(first.contains("final G1->G0 E3->E2 parents=G5->G4 task=G2/E2 winner=E2@2"));
}

use std::collections::{BTreeMap, BTreeSet};
use std::fmt::{self, Write};

pub use optimizer_blog_lesson_2::{ExprId, GroupId, Winner};

pub const DESTINATION_SCAN_GROUP: GroupId = GroupId(0);
pub const SOURCE_SCAN_GROUP: GroupId = GroupId(1);
pub const DESTINATION_PROJECT_GROUP: GroupId = GroupId(2);
pub const SOURCE_PROJECT_GROUP: GroupId = GroupId(3);
pub const DESTINATION_OUTER_GROUP: GroupId = GroupId(4);
pub const SOURCE_OUTER_GROUP: GroupId = GroupId(5);

pub const DESTINATION_PROJECT_EXPR: ExprId = ExprId(2);
pub const SOURCE_PROJECT_EXPR: ExprId = ExprId(3);

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct ExprKey {
    pub op: &'static str,
    pub children: Vec<GroupId>,
}

impl ExprKey {
    pub fn new(op: &'static str, children: Vec<GroupId>) -> Self {
        Self { op, children }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct TaskHandle {
    pub group_id: GroupId,
    pub expr_id: ExprId,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum ReplayScenario {
    RedirectFirst,
    SkipRekeyAndCascade,
    SkipHandleAndWinnerRepair,
    Canonical,
}

impl ReplayScenario {
    pub const DEMO: [Self; 4] = [
        Self::RedirectFirst,
        Self::SkipRekeyAndCascade,
        Self::SkipHandleAndWinnerRepair,
        Self::Canonical,
    ];

    pub fn name(self) -> &'static str {
        match self {
            Self::RedirectFirst => "redirect-first",
            Self::SkipRekeyAndCascade => "skip-rekey-and-cascade",
            Self::SkipHandleAndWinnerRepair => "skip-handle-and-winner-repair",
            Self::Canonical => "canonical",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum JournalEntry {
    Capture {
        exprs: Vec<ExprId>,
        parents: Vec<ExprId>,
    },
    MigrateExpr(ExprId),
    RedirectGroup {
        from: GroupId,
        to: GroupId,
    },
    RekeyExpr {
        expr_id: ExprId,
        from: GroupId,
        to: GroupId,
    },
    RedirectExpr {
        from: ExprId,
        to: ExprId,
    },
    ResolveTask {
        from: TaskHandle,
        to: TaskHandle,
    },
    WriteWinner {
        handle: TaskHandle,
        cost: u64,
    },
    Skipped(&'static str),
}

impl fmt::Display for JournalEntry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Capture { exprs, parents } => write!(
                f,
                "capture G1 exprs={} parents={}",
                expr_ids(exprs),
                expr_ids(parents)
            ),
            Self::MigrateExpr(expr_id) => write!(f, "migrate E{} G1->G0", expr_id.0),
            Self::RedirectGroup { from, to } => {
                write!(f, "redirect G{}->G{}", from.0, to.0)
            }
            Self::RekeyExpr { expr_id, from, to } => {
                write!(f, "rekey E{} G{}->G{}", expr_id.0, from.0, to.0)
            }
            Self::RedirectExpr { from, to } => {
                write!(f, "redirect E{}->E{}", from.0, to.0)
            }
            Self::ResolveTask { from, to } => write!(
                f,
                "resolve task G{}/E{}->G{}/E{}",
                from.group_id.0, from.expr_id.0, to.group_id.0, to.expr_id.0
            ),
            Self::WriteWinner { handle, cost } => write!(
                f,
                "winner G{}=E{}@{}",
                handle.group_id.0, handle.expr_id.0, cost
            ),
            Self::Skipped(step) => write!(f, "skip {step}"),
        }
    }
}

fn expr_ids(ids: &[ExprId]) -> String {
    let body = ids
        .iter()
        .map(|id| format!("E{}", id.0))
        .collect::<Vec<_>>()
        .join(",");
    format!("[{body}]")
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Failure {
    SourceExpressionNotMigrated {
        expr_id: ExprId,
        from: GroupId,
        to: GroupId,
    },
    NonCanonicalChild {
        expr_id: ExprId,
        child: GroupId,
        representative: GroupId,
    },
    DuplicateCanonicalExpression {
        first: ExprId,
        second: ExprId,
    },
    ParentGroupsDidNotConverge {
        first: GroupId,
        second: GroupId,
    },
    StaleTaskWrite(TaskHandle),
    ExpensiveWinnerRetained {
        cost: u64,
    },
    BacklinkMismatch {
        group_id: GroupId,
    },
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SourceExpressionNotMigrated { expr_id, from, to } => write!(
                f,
                "E{} stayed in redirected G{} instead of G{}",
                expr_id.0, from.0, to.0
            ),
            Self::NonCanonicalChild {
                expr_id,
                child,
                representative,
            } => write!(
                f,
                "E{} still uses G{} instead of G{}",
                expr_id.0, child.0, representative.0
            ),
            Self::DuplicateCanonicalExpression { first, second } => write!(
                f,
                "E{} and E{} have the same canonical key",
                first.0, second.0
            ),
            Self::ParentGroupsDidNotConverge { first, second } => {
                write!(f, "G{} and G{} did not converge", first.0, second.0)
            }
            Self::StaleTaskWrite(handle) => write!(
                f,
                "task wrote through stale G{}/E{}",
                handle.group_id.0, handle.expr_id.0
            ),
            Self::ExpensiveWinnerRetained { cost } => {
                write!(f, "winner cost is {cost}, expected 2")
            }
            Self::BacklinkMismatch { group_id } => {
                write!(f, "full scan and backlinks disagree for G{}", group_id.0)
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Snapshot {
    exprs: BTreeMap<ExprId, ExprKey>,
    owners: BTreeMap<ExprId, GroupId>,
    group_redirects: BTreeMap<GroupId, GroupId>,
    expr_redirects: BTreeMap<ExprId, ExprId>,
    winners: BTreeMap<GroupId, Winner>,
    parent_exprs: BTreeMap<GroupId, BTreeSet<ExprId>>,
    original_task: TaskHandle,
    resolved_task: Option<TaskHandle>,
    stale_writes: Vec<TaskHandle>,
    include_outer_parents: bool,
}

impl Snapshot {
    pub fn representative_group(&self, mut group_id: GroupId) -> GroupId {
        loop {
            let next = self.group_redirects[&group_id];
            if next == group_id {
                return group_id;
            }
            group_id = next;
        }
    }

    pub fn representative_expr(&self, mut expr_id: ExprId) -> ExprId {
        while let Some(&next) = self.expr_redirects.get(&expr_id) {
            expr_id = next;
        }
        expr_id
    }

    pub fn owner_of(&self, expr_id: ExprId) -> Option<GroupId> {
        let expr_id = self.representative_expr(expr_id);
        self.owners
            .get(&expr_id)
            .map(|group_id| self.representative_group(*group_id))
    }

    pub fn winner(&self, group_id: GroupId) -> Option<Winner> {
        let group_id = self.representative_group(group_id);
        self.winners.get(&group_id).map(|winner| Winner {
            expr_id: self.representative_expr(winner.expr_id),
            cost: winner.cost,
        })
    }

    pub fn original_task(&self) -> TaskHandle {
        self.original_task
    }

    pub fn resolved_task(&self) -> Option<TaskHandle> {
        self.resolved_task
    }

    pub fn full_scan_parents(&self, group_id: GroupId) -> Vec<ExprId> {
        self.exprs
            .iter()
            .filter_map(|(expr_id, expr)| expr.children.contains(&group_id).then_some(*expr_id))
            .collect()
    }

    pub fn backlink_parents(&self, group_id: GroupId) -> Vec<ExprId> {
        self.parent_exprs
            .get(&group_id)
            .map(|parents| parents.iter().copied().collect())
            .unwrap_or_default()
    }

    pub fn canonical_owner_count(&self, wanted: &ExprKey) -> usize {
        let wanted = self.canonical_key(wanted);
        self.exprs
            .values()
            .filter(|expr| self.canonical_key(expr) == wanted)
            .count()
    }

    pub fn failures(&self) -> Vec<Failure> {
        let mut failures = Vec::new();
        if self.representative_group(SOURCE_SCAN_GROUP) == DESTINATION_SCAN_GROUP
            && self.owners.get(&ExprId(1)) == Some(&SOURCE_SCAN_GROUP)
        {
            failures.push(Failure::SourceExpressionNotMigrated {
                expr_id: ExprId(1),
                from: SOURCE_SCAN_GROUP,
                to: DESTINATION_SCAN_GROUP,
            });
        }

        for (&expr_id, expr) in &self.exprs {
            for &child in &expr.children {
                let representative = self.representative_group(child);
                if child != representative {
                    failures.push(Failure::NonCanonicalChild {
                        expr_id,
                        child,
                        representative,
                    });
                }
            }
        }

        let mut by_key = BTreeMap::<ExprKey, ExprId>::new();
        for (&expr_id, expr) in &self.exprs {
            let key = self.canonical_key(expr);
            if let Some(&first) = by_key.get(&key) {
                failures.push(Failure::DuplicateCanonicalExpression {
                    first,
                    second: expr_id,
                });
            } else {
                by_key.insert(key, expr_id);
            }
        }

        if self.include_outer_parents
            && self.representative_group(DESTINATION_OUTER_GROUP)
                != self.representative_group(SOURCE_OUTER_GROUP)
        {
            failures.push(Failure::ParentGroupsDidNotConverge {
                first: DESTINATION_OUTER_GROUP,
                second: SOURCE_OUTER_GROUP,
            });
        }
        failures.extend(
            self.stale_writes
                .iter()
                .copied()
                .map(Failure::StaleTaskWrite),
        );
        if self.representative_group(DESTINATION_PROJECT_GROUP)
            == self.representative_group(SOURCE_PROJECT_GROUP)
            && let Some(winner) = self.winner(DESTINATION_PROJECT_GROUP)
            && winner.cost != 2
        {
            failures.push(Failure::ExpensiveWinnerRetained { cost: winner.cost });
        }
        for &group_id in self.group_redirects.keys() {
            if self.full_scan_parents(group_id) != self.backlink_parents(group_id) {
                failures.push(Failure::BacklinkMismatch { group_id });
            }
        }
        failures
    }

    fn canonical_key(&self, expr: &ExprKey) -> ExprKey {
        ExprKey::new(
            expr.op,
            expr.children
                .iter()
                .map(|group_id| self.representative_group(*group_id))
                .collect(),
        )
    }

    fn rebuild_backlinks(&mut self) {
        self.parent_exprs.clear();
        for (&expr_id, expr) in &self.exprs {
            for &child in &expr.children {
                self.parent_exprs.entry(child).or_default().insert(expr_id);
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ReplayResult {
    pub scenario: ReplayScenario,
    pub journal: Vec<JournalEntry>,
    pub snapshot: Snapshot,
    pub failures: Vec<Failure>,
}

impl ReplayResult {
    pub fn render(&self) -> String {
        let mut output = format!("scenario={}\n", self.scenario.name());
        for entry in &self.journal {
            let _ = writeln!(output, "  {entry}");
        }
        if self.failures.is_empty() {
            let task = self.snapshot.resolved_task().unwrap();
            let winner = self.snapshot.winner(DESTINATION_PROJECT_GROUP).unwrap();
            let _ = writeln!(
                output,
                "  final G1->G{} E3->E{} parents=G5->G{} task=G{}/E{} winner=E{}@{}",
                self.snapshot.representative_group(SOURCE_SCAN_GROUP).0,
                self.snapshot.representative_expr(SOURCE_PROJECT_EXPR).0,
                self.snapshot.representative_group(SOURCE_OUTER_GROUP).0,
                task.group_id.0,
                task.expr_id.0,
                winner.expr_id.0,
                winner.cost
            );
        } else {
            for failure in &self.failures {
                let _ = writeln!(output, "  failure {failure}");
            }
        }
        output
    }
}

#[derive(Copy, Clone)]
enum WinnerPolicy {
    Cheapest,
    KeepDestination,
}

struct Transaction {
    snapshot: Snapshot,
    journal: Vec<JournalEntry>,
}

impl Transaction {
    fn new(include_outer_parents: bool) -> Self {
        let mut exprs = BTreeMap::from([
            (ExprId(0), ExprKey::new("scan:a", vec![])),
            (ExprId(1), ExprKey::new("scan:b", vec![])),
            (
                ExprId(2),
                ExprKey::new("project:x", vec![DESTINATION_SCAN_GROUP]),
            ),
            (
                ExprId(3),
                ExprKey::new("project:x", vec![SOURCE_SCAN_GROUP]),
            ),
        ]);
        let mut owners = BTreeMap::from([
            (ExprId(0), DESTINATION_SCAN_GROUP),
            (ExprId(1), SOURCE_SCAN_GROUP),
            (ExprId(2), DESTINATION_PROJECT_GROUP),
            (ExprId(3), SOURCE_PROJECT_GROUP),
        ]);
        if include_outer_parents {
            exprs.insert(
                ExprId(4),
                ExprKey::new("project:y", vec![DESTINATION_PROJECT_GROUP]),
            );
            exprs.insert(
                ExprId(5),
                ExprKey::new("project:y", vec![SOURCE_PROJECT_GROUP]),
            );
            owners.insert(ExprId(4), DESTINATION_OUTER_GROUP);
            owners.insert(ExprId(5), SOURCE_OUTER_GROUP);
        }
        let group_count = if include_outer_parents { 6 } else { 4 };
        let group_redirects = (0..group_count)
            .map(|id| (GroupId(id), GroupId(id)))
            .collect();
        let mut snapshot = Snapshot {
            exprs,
            owners,
            group_redirects,
            expr_redirects: BTreeMap::new(),
            winners: BTreeMap::from([
                (
                    DESTINATION_PROJECT_GROUP,
                    Winner {
                        expr_id: DESTINATION_PROJECT_EXPR,
                        cost: 10,
                    },
                ),
                (
                    SOURCE_PROJECT_GROUP,
                    Winner {
                        expr_id: SOURCE_PROJECT_EXPR,
                        cost: 2,
                    },
                ),
            ]),
            parent_exprs: BTreeMap::new(),
            original_task: TaskHandle {
                group_id: SOURCE_PROJECT_GROUP,
                expr_id: SOURCE_PROJECT_EXPR,
            },
            resolved_task: None,
            stale_writes: Vec::new(),
            include_outer_parents,
        };
        snapshot.rebuild_backlinks();
        Self {
            snapshot,
            journal: Vec::new(),
        }
    }

    fn capture(&mut self) {
        self.journal.push(JournalEntry::Capture {
            exprs: vec![ExprId(1)],
            parents: self.snapshot.backlink_parents(SOURCE_SCAN_GROUP),
        });
    }

    fn migrate_source(&mut self) {
        if self.snapshot.representative_group(SOURCE_SCAN_GROUP) != SOURCE_SCAN_GROUP {
            self.journal
                .push(JournalEntry::Skipped("source migration after redirect"));
            return;
        }
        self.snapshot
            .owners
            .insert(ExprId(1), DESTINATION_SCAN_GROUP);
        self.journal.push(JournalEntry::MigrateExpr(ExprId(1)));
    }

    fn redirect_group(&mut self, from: GroupId, to: GroupId) {
        self.snapshot.group_redirects.insert(from, to);
        self.journal.push(JournalEntry::RedirectGroup { from, to });
    }

    fn rekey_and_converge(&mut self, winner_policy: WinnerPolicy) {
        self.rekey_expr(
            SOURCE_PROJECT_EXPR,
            SOURCE_SCAN_GROUP,
            DESTINATION_SCAN_GROUP,
        );
        self.redirect_expr(SOURCE_PROJECT_EXPR, DESTINATION_PROJECT_EXPR);
        self.merge_winners(winner_policy);
        self.redirect_group(SOURCE_PROJECT_GROUP, DESTINATION_PROJECT_GROUP);

        if self.snapshot.include_outer_parents {
            self.rekey_expr(ExprId(5), SOURCE_PROJECT_GROUP, DESTINATION_PROJECT_GROUP);
            self.redirect_expr(ExprId(5), ExprId(4));
            self.redirect_group(SOURCE_OUTER_GROUP, DESTINATION_OUTER_GROUP);
        }
        self.snapshot.rebuild_backlinks();
    }

    fn rekey_expr(&mut self, expr_id: ExprId, from: GroupId, to: GroupId) {
        self.snapshot.exprs.get_mut(&expr_id).unwrap().children = vec![to];
        self.journal
            .push(JournalEntry::RekeyExpr { expr_id, from, to });
    }

    fn redirect_expr(&mut self, from: ExprId, to: ExprId) {
        self.snapshot.exprs.remove(&from);
        self.snapshot.owners.remove(&from);
        self.snapshot.expr_redirects.insert(from, to);
        self.journal.push(JournalEntry::RedirectExpr { from, to });
    }

    fn merge_winners(&mut self, policy: WinnerPolicy) {
        let destination = self
            .snapshot
            .winners
            .remove(&DESTINATION_PROJECT_GROUP)
            .unwrap();
        let source = self.snapshot.winners.remove(&SOURCE_PROJECT_GROUP).unwrap();
        let winner = match policy {
            WinnerPolicy::Cheapest if source.cost < destination.cost => source,
            WinnerPolicy::Cheapest | WinnerPolicy::KeepDestination => destination,
        };
        self.snapshot.winners.insert(
            DESTINATION_PROJECT_GROUP,
            Winner {
                expr_id: self.snapshot.representative_expr(winner.expr_id),
                cost: winner.cost,
            },
        );
    }

    fn repair_task_and_write(&mut self, canonical: bool) {
        let original = self.snapshot.original_task;
        if canonical {
            let resolved = TaskHandle {
                group_id: self.snapshot.representative_group(original.group_id),
                expr_id: self.snapshot.representative_expr(original.expr_id),
            };
            self.snapshot.resolved_task = Some(resolved);
            let winner = self.snapshot.winners[&resolved.group_id];
            self.journal.push(JournalEntry::ResolveTask {
                from: original,
                to: resolved,
            });
            self.journal.push(JournalEntry::WriteWinner {
                handle: resolved,
                cost: winner.cost,
            });
        } else {
            self.snapshot.stale_writes.push(original);
            self.snapshot.winners.insert(
                original.group_id,
                Winner {
                    expr_id: original.expr_id,
                    cost: 2,
                },
            );
            self.journal
                .push(JournalEntry::Skipped("task-handle reduction"));
            self.journal.push(JournalEntry::WriteWinner {
                handle: original,
                cost: 2,
            });
        }
    }

    fn finish(self, scenario: ReplayScenario) -> ReplayResult {
        let failures = self.snapshot.failures();
        ReplayResult {
            scenario,
            journal: self.journal,
            snapshot: self.snapshot,
            failures,
        }
    }
}

pub fn initial_snapshot(include_outer_parents: bool) -> Snapshot {
    Transaction::new(include_outer_parents).snapshot
}

pub fn replay(scenario: ReplayScenario, include_outer_parents: bool) -> ReplayResult {
    let mut transaction = Transaction::new(include_outer_parents);
    match scenario {
        ReplayScenario::RedirectFirst => {
            transaction.redirect_group(SOURCE_SCAN_GROUP, DESTINATION_SCAN_GROUP);
            transaction.capture();
            transaction.migrate_source();
        }
        ReplayScenario::SkipRekeyAndCascade => {
            transaction.capture();
            transaction.migrate_source();
            transaction.redirect_group(SOURCE_SCAN_GROUP, DESTINATION_SCAN_GROUP);
            transaction
                .journal
                .push(JournalEntry::Skipped("parent re-key and collision cascade"));
        }
        ReplayScenario::SkipHandleAndWinnerRepair => {
            transaction.capture();
            transaction.migrate_source();
            transaction.redirect_group(SOURCE_SCAN_GROUP, DESTINATION_SCAN_GROUP);
            transaction.rekey_and_converge(WinnerPolicy::KeepDestination);
            transaction.repair_task_and_write(false);
        }
        ReplayScenario::Canonical => {
            transaction.capture();
            transaction.migrate_source();
            transaction.redirect_group(SOURCE_SCAN_GROUP, DESTINATION_SCAN_GROUP);
            transaction.rekey_and_converge(WinnerPolicy::Cheapest);
            transaction.repair_task_and_write(true);
        }
    }
    transaction.finish(scenario)
}

pub fn render_demo() -> String {
    ReplayScenario::DEMO
        .into_iter()
        .map(|scenario| replay(scenario, true).render())
        .collect::<Vec<_>>()
        .join("\n")
}

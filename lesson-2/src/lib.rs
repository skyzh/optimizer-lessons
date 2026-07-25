use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;

pub mod s01_shallow_merge;
pub mod s02_rewrite_children;
pub mod s03_cascading_merge;
pub mod s04_stable_handles;

#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct GroupId(pub usize);

#[derive(Copy, Clone, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct ExprId(pub usize);

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub enum RelNodeType {
    Scan(&'static str),
    Project(&'static str),
    Filter(&'static str),
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct RelNode {
    pub typ: RelNodeType,
    pub children: Vec<Arc<RelNode>>,
}

pub fn scan(table: &'static str) -> RelNode {
    RelNode {
        typ: RelNodeType::Scan(table),
        children: vec![],
    }
}

pub fn project(child: impl Into<Arc<RelNode>>, columns: &'static str) -> RelNode {
    RelNode {
        typ: RelNodeType::Project(columns),
        children: vec![child.into()],
    }
}

pub fn filter(child: impl Into<Arc<RelNode>>, predicate: &'static str) -> RelNode {
    RelNode {
        typ: RelNodeType::Filter(predicate),
        children: vec![child.into()],
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct MemoExpr {
    pub typ: RelNodeType,
    pub children: Vec<GroupId>,
}

impl MemoExpr {
    pub fn new(typ: RelNodeType, children: Vec<GroupId>) -> Self {
        Self { typ, children }
    }
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub struct Winner {
    pub expr_id: ExprId,
    pub cost: u64,
}

#[derive(Default)]
struct Group {
    exprs: BTreeSet<ExprId>,
    winner: Option<Winner>,
}

pub struct Memo {
    groups: HashMap<GroupId, Group>,
    exprs: HashMap<ExprId, MemoExpr>,

    // Secondary indexes. Keeping these synchronized is most of the difficulty.
    expr_to_id: HashMap<MemoExpr, ExprId>,
    expr_to_group: HashMap<ExprId, GroupId>,

    // Optimizer tasks can outlive a merge, so old IDs remain valid handles.
    group_redirect: HashMap<GroupId, GroupId>,
    expr_redirect: HashMap<ExprId, ExprId>,

    next_group_id: usize,
    next_expr_id: usize,
}

impl Default for Memo {
    fn default() -> Self {
        Self::new()
    }
}

impl Memo {
    pub fn new() -> Self {
        Self {
            groups: HashMap::new(),
            exprs: HashMap::new(),
            expr_to_id: HashMap::new(),
            expr_to_group: HashMap::new(),
            group_redirect: HashMap::new(),
            expr_redirect: HashMap::new(),
            next_group_id: 0,
            next_expr_id: 0,
        }
    }

    pub fn add_plan(&mut self, node: Arc<RelNode>) -> (GroupId, ExprId) {
        let children = node
            .children
            .iter()
            .map(|child| self.add_plan(child.clone()).0)
            .collect();
        self.add_expr(MemoExpr::new(node.typ.clone(), children))
    }

    pub fn add_expr(&mut self, expr: MemoExpr) -> (GroupId, ExprId) {
        self.add_expr_inner(expr, None)
    }

    pub fn add_expr_to_group(&mut self, expr: MemoExpr, group_id: GroupId) -> (GroupId, ExprId) {
        self.add_expr_inner(expr, Some(group_id))
    }

    fn add_expr_inner(
        &mut self,
        expr: MemoExpr,
        target_group: Option<GroupId>,
    ) -> (GroupId, ExprId) {
        let expr = self.canonicalize_expr(expr);
        if let Some(&expr_id) = self.expr_to_id.get(&expr) {
            let expr_id = self.representative_expr(expr_id);
            let existing_group = self.group_of_expr(expr_id);
            if let Some(target_group) = target_group {
                let target_group = self.representative(target_group);
                if target_group != existing_group {
                    let merged = self.merge_group(target_group, existing_group);
                    return (merged, expr_id);
                }
            }
            return (existing_group, expr_id);
        }

        let expr_id = ExprId(self.next_expr_id);
        self.next_expr_id += 1;
        let group_id = match target_group {
            Some(group_id) => self.representative(group_id),
            None => self.new_group(),
        };

        self.exprs.insert(expr_id, expr.clone());
        self.expr_to_id.insert(expr, expr_id);
        self.expr_to_group.insert(expr_id, group_id);
        self.groups
            .get_mut(&group_id)
            .unwrap()
            .exprs
            .insert(expr_id);
        (group_id, expr_id)
    }

    fn new_group(&mut self) -> GroupId {
        let group_id = GroupId(self.next_group_id);
        self.next_group_id += 1;
        self.groups.insert(group_id, Group::default());
        self.group_redirect.insert(group_id, group_id);
        group_id
    }

    pub fn representative(&self, mut group_id: GroupId) -> GroupId {
        loop {
            let next = self.group_redirect[&group_id];
            if next == group_id {
                return group_id;
            }
            group_id = next;
        }
    }

    pub fn representative_expr(&self, mut expr_id: ExprId) -> ExprId {
        while let Some(&next) = self.expr_redirect.get(&expr_id) {
            expr_id = next;
        }
        expr_id
    }

    pub fn group_of_expr(&self, expr_id: ExprId) -> GroupId {
        let expr_id = self.representative_expr(expr_id);
        self.representative(self.expr_to_group[&expr_id])
    }

    pub fn expr(&self, expr_id: ExprId) -> &MemoExpr {
        let expr_id = self.representative_expr(expr_id);
        &self.exprs[&expr_id]
    }

    pub fn exprs_in_group(&self, group_id: GroupId) -> Vec<(ExprId, MemoExpr)> {
        let group_id = self.representative(group_id);
        self.groups[&group_id]
            .exprs
            .iter()
            .map(|expr_id| (*expr_id, self.exprs[expr_id].clone()))
            .collect()
    }

    pub fn group_count(&self) -> usize {
        self.groups.len()
    }

    pub fn expression_count(&self) -> usize {
        self.exprs.len()
    }

    pub fn groups_containing(&self, wanted: &MemoExpr) -> Vec<GroupId> {
        let wanted = self.canonicalize_expr(wanted.clone());
        let mut groups = self
            .exprs
            .iter()
            .filter(|(_, expr)| *expr == &wanted)
            .map(|(expr_id, _)| self.group_of_expr(*expr_id))
            .collect::<Vec<_>>();
        groups.sort();
        groups.dedup();
        groups
    }

    pub fn set_winner(&mut self, group_id: GroupId, expr_id: ExprId, cost: u64) {
        let group_id = self.representative(group_id);
        let expr_id = self.representative_expr(expr_id);
        assert_eq!(self.group_of_expr(expr_id), group_id);
        self.groups.get_mut(&group_id).unwrap().winner = Some(Winner { expr_id, cost });
    }

    pub fn winner(&self, group_id: GroupId) -> Option<Winner> {
        let group_id = self.representative(group_id);
        self.groups[&group_id].winner.map(|winner| Winner {
            expr_id: self.representative_expr(winner.expr_id),
            cost: winner.cost,
        })
    }

    fn canonicalize_expr(&self, mut expr: MemoExpr) -> MemoExpr {
        for child in &mut expr.children {
            *child = self.representative(*child);
        }
        expr
    }

    fn move_group(&mut self, merge_into: GroupId, merge_from: GroupId) -> GroupId {
        let merge_into = self.representative(merge_into);
        let merge_from = self.representative(merge_from);
        if merge_into == merge_from {
            return merge_into;
        }

        let mut from_group = self.groups.remove(&merge_from).unwrap();
        let into_group = self.groups.get_mut(&merge_into).unwrap();
        for expr_id in &from_group.exprs {
            self.expr_to_group.insert(*expr_id, merge_into);
        }
        into_group.exprs.append(&mut from_group.exprs);
        into_group.winner = match (into_group.winner, from_group.winner) {
            (None, winner) | (winner, None) => winner,
            (Some(left), Some(right)) => Some(if left.cost <= right.cost { left } else { right }),
        };

        self.group_redirect.insert(merge_from, merge_into);
        for target in self.group_redirect.values_mut() {
            if *target == merge_from {
                *target = merge_into;
            }
        }
        merge_into
    }

    fn redirect_expr(&mut self, from: ExprId, to: ExprId) {
        self.expr_redirect.insert(from, to);
        for target in self.expr_redirect.values_mut() {
            if *target == from {
                *target = to;
            }
        }
        for group in self.groups.values_mut() {
            if let Some(winner) = &mut group.winner {
                if winner.expr_id == from {
                    winner.expr_id = to;
                }
            }
        }
    }

    pub fn check_invariants(&self) -> Result<(), String> {
        let representatives = self.groups.keys().copied().collect::<HashSet<_>>();
        for (&id, &target) in &self.group_redirect {
            let representative = self.representative(id);
            if !representatives.contains(&representative) {
                return Err(format!(
                    "group {id:?} redirects to missing group {representative:?}"
                ));
            }
            if self.representative(target) != representative {
                return Err(format!("group {id:?} has a broken redirect chain"));
            }
        }

        if self.exprs.len() != self.expr_to_id.len() || self.exprs.len() != self.expr_to_group.len()
        {
            return Err("expression indexes have different sizes".to_string());
        }

        let mut seen = HashSet::new();
        for (&group_id, group) in &self.groups {
            if group.exprs.is_empty() {
                return Err(format!("group {group_id:?} is empty"));
            }
            for &expr_id in &group.exprs {
                if !seen.insert(expr_id) {
                    return Err(format!("expression {expr_id:?} belongs to two groups"));
                }
                if self.expr_to_group.get(&expr_id) != Some(&group_id) {
                    return Err(format!("expression {expr_id:?} has the wrong group index"));
                }
            }
            if let Some(winner) = group.winner {
                let winner_id = self.representative_expr(winner.expr_id);
                if self.group_of_expr(winner_id) != group_id {
                    return Err(format!("winner {winner_id:?} belongs to another group"));
                }
            }
        }

        if seen.len() != self.exprs.len() {
            return Err("some expressions are not in a group".to_string());
        }
        for (&expr_id, expr) in &self.exprs {
            if self.expr_to_id.get(expr) != Some(&expr_id) {
                return Err(format!("expression {expr_id:?} has a stale reverse index"));
            }
            for &child in &expr.children {
                if child != self.representative(child) {
                    return Err(format!(
                        "expression {expr_id:?} contains stale child {child:?}"
                    ));
                }
                if !representatives.contains(&child) {
                    return Err(format!(
                        "expression {expr_id:?} refers to missing child {child:?}"
                    ));
                }
            }
        }
        Ok(())
    }
}

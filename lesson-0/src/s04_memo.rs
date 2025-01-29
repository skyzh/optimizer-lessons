use std::{collections::HashMap, sync::Arc};

use super::*;

pub type MemoScan = Scan;
pub type MemoColumnRefPred = ColumnRefPred;
pub type MemoConstPred = ConstPred;

#[derive(Copy, Debug, Clone, Hash, Eq, PartialEq)]
pub struct GroupId(usize);

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct MemoJoin {
    pub left: GroupId,
    pub right: GroupId,
    pub cond: GroupId,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct MemoFilter {
    pub child: GroupId,
    pub predicate: GroupId,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct MemoEqPred {
    pub left: GroupId,
    pub right: GroupId,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum MemoRelNode {
    Scan(MemoScan),
    Join(MemoJoin),
    Filter(MemoFilter),
    Eq(MemoEqPred),
    ColumnRef(MemoColumnRefPred),
    Const(MemoConstPred),
}

pub struct Memo {
    groups: Vec<Vec<MemoRelNode>>,
    expr_to_group: HashMap<MemoRelNode, GroupId>,
}

impl Memo {
    pub fn add_expr(&mut self, expr: MemoRelNode) -> GroupId {
        if let Some(group_id) = self.get_group(expr.clone()) {
            return group_id;
        }
        let id = GroupId(self.groups.len());
        self.groups.push(vec![expr.clone()]);
        self.expr_to_group.insert(expr, id);
        id
    }

    pub fn get_group(&self, expr: MemoRelNode) -> Option<GroupId> {
        self.expr_to_group.get(&expr).copied()
    }

    pub fn new() -> Self {
        Self {
            groups: vec![],
            expr_to_group: HashMap::new(),
        }
    }

    pub fn dump(&self) {
        for (i, group) in self.groups.iter().enumerate() {
            println!("Group {}", i);
            for expr in group {
                println!("  {:?}", expr);
            }
        }
    }

    pub fn get_all_exprs_in_group(&self, group: GroupId) -> Vec<MemoRelNode> {
        self.groups[group.0].iter().cloned().collect()
    }

    pub fn merge_group(&mut self, group1: GroupId, group2: GroupId) -> GroupId {
        unimplemented!()
    }

    pub fn add_expr_to_group(&mut self, group: GroupId, expr: MemoRelNode) {
        unimplemented!()
    }
}

pub fn memorize_rel(memo: &mut Memo, rel: Arc<RelNode>) -> GroupId {
    let rel = match &*rel {
        RelNode::Scan(scan) => MemoRelNode::Scan(scan.clone()),
        RelNode::Join(join) => MemoRelNode::Join(MemoJoin {
            left: memorize_rel(memo, join.left.clone()),
            right: memorize_rel(memo, join.right.clone()),
            cond: memorize_rel(memo, join.cond.clone()),
        }),
        RelNode::Filter(filter) => MemoRelNode::Filter(MemoFilter {
            child: memorize_rel(memo, filter.child.clone()),
            predicate: memorize_rel(memo, filter.predicate.clone()),
        }),
        RelNode::Eq(eq) => MemoRelNode::Eq(MemoEqPred {
            left: memorize_rel(memo, eq.left.clone()),
            right: memorize_rel(memo, eq.right.clone()),
        }),
        RelNode::ColumnRef(column_ref) => MemoRelNode::ColumnRef(column_ref.clone()),
        RelNode::Const(const_pred) => MemoRelNode::Const(const_pred.clone()),
        // ... doesn't seem maintainable
    };
    memo.add_expr(rel)
}

pub fn generate_one_binding(memo: &Memo, group: GroupId) -> Arc<RelNode> {
    let expr = &memo.groups[group.0][0];
    match expr {
        MemoRelNode::Scan(scan) => Arc::new(RelNode::Scan(scan.clone())),
        MemoRelNode::Join(join) => Arc::new(RelNode::Join(Join {
            left: generate_one_binding(memo, join.left),
            right: generate_one_binding(memo, join.right),
            cond: generate_one_binding(memo, join.cond),
        })),
        MemoRelNode::Filter(filter) => Arc::new(RelNode::Filter(Filter {
            child: generate_one_binding(memo, filter.child),
            predicate: generate_one_binding(memo, filter.predicate),
        })),
        MemoRelNode::Eq(eq) => Arc::new(RelNode::Eq(EqPred {
            left: generate_one_binding(memo, eq.left),
            right: generate_one_binding(memo, eq.right),
        })),
        MemoRelNode::ColumnRef(column_ref) => Arc::new(RelNode::ColumnRef(column_ref.clone())),
        MemoRelNode::Const(const_pred) => Arc::new(RelNode::Const(const_pred.clone())),
        // ... doesn't seem maintainable
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memorize_rel() {
        let mut memo = Memo {
            groups: vec![],
            expr_to_group: HashMap::new(),
        };

        let rel = join(
            // Do a self-join
            filter(scan(TableId(0)), eq_pred(column_ref_pred(1), const_pred(3))),
            filter(scan(TableId(0)), eq_pred(column_ref_pred(1), const_pred(3))),
            eq_pred(column_ref_pred(1), column_ref_pred(3)),
        );

        let group_id = memorize_rel(&mut memo, Arc::new(rel.clone()));
        memo.dump();

        assert_eq!(memo.groups.len(), 8);
        assert_eq!(generate_one_binding(&memo, group_id).as_ref(), &rel);
    }
}

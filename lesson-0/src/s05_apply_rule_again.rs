use std::sync::Arc;

use super::*;

pub type BindScan = Scan;
pub type BindColumnRefPred = ColumnRefPred;
pub type BindConstPred = ConstPred;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct BindJoin {
    pub left: Arc<BindRelNode>,
    pub right: Arc<BindRelNode>,
    pub cond: Arc<BindRelNode>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct BindFilter {
    pub child: Arc<BindRelNode>,
    pub predicate: Arc<BindRelNode>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct BindEqPred {
    pub left: Arc<BindRelNode>,
    pub right: Arc<BindRelNode>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum BindRelNode {
    Scan(BindScan),
    Join(BindJoin),
    Filter(BindFilter),
    Eq(BindEqPred),
    ColumnRef(BindColumnRefPred),
    Const(BindConstPred),
    Group(GroupId),
}

pub fn join_commute(node: Arc<BindRelNode>) -> Option<Arc<BindRelNode>> {
    if let BindRelNode::Join(ref a) = &*node {
        // TODO: rewrite the condition
        return Some(Arc::new(BindRelNode::Join(BindJoin {
            right: a.left.clone(),
            left: a.right.clone(),
            cond: a.cond.clone(),
        })));
    }
    None
}

pub fn join_assoc(node: Arc<BindRelNode>) -> Option<Arc<BindRelNode>> {
    if let BindRelNode::Join(ref a) = &*node {
        if let BindRelNode::Join(b) = &*a.left {
            return Some(Arc::new(BindRelNode::Join(BindJoin {
                left: b.left.clone(),
                right: Arc::new(BindRelNode::Join(BindJoin {
                    left: b.right.clone(),
                    right: a.right.clone(),
                    cond: a.cond.clone(),
                })),
                cond: b.cond.clone(),
            })));
        }
    }
    None
}

pub fn apply_join_commute_rules_on_node(memo: &mut Memo, group: GroupId, node: MemoRelNode) {
    if let MemoRelNode::Join(node) = node {
        let binding = BindJoin {
            left: Arc::new(BindRelNode::Group(node.left)),
            right: Arc::new(BindRelNode::Group(node.right)),
            cond: Arc::new(BindRelNode::Group(node.cond)),
        };
        let applied = join_commute(Arc::new(BindRelNode::Join(binding))).unwrap();
        add_binding_to_memo(memo, group, applied);
    }
}

pub fn apply_join_assoc_rules_on_node(memo: &mut Memo, group: GroupId, node: MemoRelNode) {
    if let MemoRelNode::Join(node1) = node {
        for expr in memo.get_all_exprs_in_group(node.left) {
            if let MemoRelNode::Join(node2) = expr {
                let binding = BindJoin {
                    left: Arc::new(BindRelNode::Join(BindJoin {
                        left: Arc::new(BindRelNode::Group(node2.left)),
                        right: Arc::new(BindRelNode::Group(node2.right)),
                        cond: Arc::new(BindRelNode::Group(node2.cond)),
                    })),
                    right: Arc::new(BindRelNode::Group(node1.right)),
                    cond: Arc::new(BindRelNode::Group(node1.cond)),
                };
                let applied = join_assoc(Arc::new(BindRelNode::Join(binding))).unwrap();
                add_binding_to_memo(memo, group, applied);
            }
        }
    }
}

pub fn add_binding_to_memo(memo: &mut Memo, group: GroupId, node: Arc<BindRelNode>) -> GroupId {
    fn add_binding_to_memo_inner(memo: &mut Memo, node: Arc<BindRelNode>) -> GroupId {
        let node = match &*node {
            BindRelNode::Scan(scan) => MemoRelNode::Scan(scan.clone()),
            BindRelNode::Join(join) => {
                let left = add_binding_to_memo_inner(memo, join.left);
                let right = add_binding_to_memo_inner(memo, join.right);
                let cond = add_binding_to_memo_inner(memo, join.cond);
                MemoRelNode::Join(MemoJoin { left, right, cond })
            }
            BindRelNode::Filter(filter) => {
                let child = add_binding_to_memo_inner(memo, filter.child);
                let predicate = add_binding_to_memo_inner(memo, filter.predicate);
                MemoRelNode::Filter(MemoFilter { child, predicate })
            }
            BindRelNode::Eq(eq) => {
                let left = add_binding_to_memo_inner(memo, eq.left);
                let right = add_binding_to_memo_inner(memo, eq.right);
                MemoRelNode::Eq(MemoEqPred { left, right })
            }
            BindRelNode::ColumnRef(column_ref) => MemoRelNode::ColumnRef(column_ref.clone()),
            BindRelNode::Const(constant) => MemoRelNode::Const(constant.clone()),
            BindRelNode::Group(group) => MemoRelNode::ColumnRef(*group),
        };
        memo.add_expr(node.clone())
    }
    let new_group = add_binding_to_memo_inner(memo, node);
    if group != new_group {
        memo.merge_group(group, new_group)
    } else {
        group
    }
}

// define repr and core, how to find a way to do both easily?

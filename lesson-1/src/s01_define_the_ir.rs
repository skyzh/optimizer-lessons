use std::sync::Arc;

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct TableId(pub usize);

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Scan {
    pub table: TableId,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Join {
    pub left: Arc<RelNode>,
    pub right: Arc<RelNode>,
    pub cond: Arc<RelNode>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct Filter {
    pub child: Arc<RelNode>,
    pub predicate: Arc<RelNode>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct EqPred {
    pub left: Arc<RelNode>,
    pub right: Arc<RelNode>,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ColumnRefPred {
    pub column: usize,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct ConstPred {
    pub value: i64,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum RelNode {
    Scan(Scan),
    Join(Join),
    Filter(Filter),
    Eq(EqPred),
    ColumnRef(ColumnRefPred),
    Const(ConstPred),
}

pub fn scan(table: TableId) -> RelNode {
    RelNode::Scan(Scan { table })
}

pub fn filter(child: impl Into<Arc<RelNode>>, cond: impl Into<Arc<RelNode>>) -> RelNode {
    RelNode::Filter(Filter {
        child: child.into(),
        predicate: cond.into(),
    })
}

pub fn join(
    left: impl Into<Arc<RelNode>>,
    right: impl Into<Arc<RelNode>>,
    cond: impl Into<Arc<RelNode>>,
) -> RelNode {
    RelNode::Join(Join {
        left: left.into(),
        right: right.into(),
        cond: cond.into(),
    })
}

pub fn eq_pred(left: impl Into<Arc<RelNode>>, right: impl Into<Arc<RelNode>>) -> RelNode {
    RelNode::Eq(EqPred {
        left: left.into(),
        right: right.into(),
    })
}

pub fn column_ref_pred(idx: usize) -> RelNode {
    RelNode::ColumnRef(ColumnRefPred { column: idx })
}

pub fn const_pred(value: i64) -> RelNode {
    RelNode::Const(ConstPred { value })
}

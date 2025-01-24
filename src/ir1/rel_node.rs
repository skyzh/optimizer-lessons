use std::sync::Arc;

#[derive(Clone)]
pub struct TableId(pub usize);

pub struct Scan {
    pub table: TableId,
}

impl Scan {
    pub fn children(&self) -> Vec<Arc<RelNode>> {
        vec![]
    }

    pub fn clone_with_children(&self, children: Vec<Arc<RelNode>>) -> Self {
        let _ = children;
        Self {
            table: self.table.clone(),
        }
    }
}

pub struct Join {
    pub left: Arc<RelNode>,
    pub right: Arc<RelNode>,
    pub cond: Arc<RelNode>,
}

impl Join {
    pub fn children(&self) -> Vec<Arc<RelNode>> {
        vec![self.left.clone(), self.right.clone(), self.cond.clone()]
    }

    pub fn clone_with_children(&self, children: Vec<Arc<RelNode>>) -> Self {
        Self {
            left: children[0].clone(),
            right: children[1].clone(),
            cond: children[2].clone(),
        }
    }
}

pub struct Filter {
    pub child: Arc<RelNode>,
    pub predicate: Arc<RelNode>,
}

impl Filter {
    pub fn children(&self) -> Vec<Arc<RelNode>> {
        vec![self.child.clone(), self.predicate.clone()]
    }

    pub fn clone_with_children(&self, children: Vec<Arc<RelNode>>) -> Self {
        Self {
            child: children[0].clone(),
            predicate: children[1].clone(),
        }
    }
}

pub struct EqPred {
    pub left: Arc<RelNode>,
    pub right: Arc<RelNode>,
}

impl EqPred {
    pub fn children(&self) -> Vec<Arc<RelNode>> {
        vec![self.left.clone(), self.right.clone()]
    }

    pub fn clone_with_children(&self, children: Vec<Arc<RelNode>>) -> Self {
        Self {
            left: children[0].clone(),
            right: children[1].clone(),
        }
    }
}

pub struct ColumnRefPred {
    pub column: usize,
}

impl ColumnRefPred {
    pub fn children(&self) -> Vec<Arc<RelNode>> {
        vec![]
    }

    pub fn clone_with_children(&self, children: Vec<Arc<RelNode>>) -> Self {
        let _ = children;
        Self {
            column: self.column,
        }
    }
}

pub struct ConstPred {
    pub value: i64,
}

impl ConstPred {
    pub fn children(&self) -> Vec<Arc<RelNode>> {
        vec![]
    }

    pub fn clone_with_children(&self, children: Vec<Arc<RelNode>>) -> Self {
        let _ = children;
        Self { value: self.value }
    }
}

pub enum RelNode {
    Scan(Scan),
    Join(Join),
    Filter(Filter),
    Eq(EqPred),
    ColumnRef(ColumnRefPred),
    Const(ConstPred),
}

impl RelNode {
    pub fn children(&self) -> Vec<Arc<RelNode>> {
        match self {
            RelNode::Scan(scan) => scan.children(),
            RelNode::Join(join) => join.children(),
            RelNode::Filter(filter) => filter.children(),
            RelNode::Eq(eq) => eq.children(),
            RelNode::ColumnRef(column_ref) => column_ref.children(),
            RelNode::Const(const_pred) => const_pred.children(),
        }
    }

    pub fn clone_with_children(&self, children: Vec<Arc<RelNode>>) -> Self {
        match self {
            RelNode::Scan(scan) => RelNode::Scan(scan.clone_with_children(children)),
            RelNode::Join(join) => RelNode::Join(join.clone_with_children(children)),
            RelNode::Filter(filter) => RelNode::Filter(filter.clone_with_children(children)),
            RelNode::Eq(eq) => RelNode::Eq(eq.clone_with_children(children)),
            RelNode::ColumnRef(column_ref) => {
                RelNode::ColumnRef(column_ref.clone_with_children(children))
            }
            RelNode::Const(const_pred) => RelNode::Const(const_pred.clone_with_children(children)),
        }
    }
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

use std::sync::Arc;

#[derive(Clone)]
pub struct TableId(pub usize);

#[derive(Clone)]
pub enum RelNodeType {
    Scan,
    Filter,
    Join,
    Eq,
    ColumnRef,
    Const,
}

pub enum RelAttrType {
    TableId(TableId),
    ColumnRef(usize),
    Const(i64),
    None,
}

pub struct RelNode {
    pub typ: RelNodeType,
    pub children: Vec<Arc<RelNode>>,
    pub data: Arc<RelAttrType>,
}

pub fn scan(table: TableId) -> RelNode {
    RelNode {
        typ: RelNodeType::Scan,
        children: vec![],
        data: Arc::new(RelAttrType::TableId(table)),
    }
}

pub fn filter(child: impl Into<Arc<RelNode>>, cond: impl Into<Arc<RelNode>>) -> RelNode {
    RelNode {
        typ: RelNodeType::Filter,
        children: vec![child.into(), cond.into()],
        data: Arc::new(RelAttrType::None),
    }
}

pub fn join(
    left: impl Into<Arc<RelNode>>,
    right: impl Into<Arc<RelNode>>,
    cond: impl Into<Arc<RelNode>>,
) -> RelNode {
    RelNode {
        typ: RelNodeType::Filter,
        children: vec![left.into(), right.into(), cond.into()],
        data: Arc::new(RelAttrType::None),
    }
}

pub fn eq_pred(left: impl Into<Arc<RelNode>>, right: impl Into<Arc<RelNode>>) -> RelNode {
    RelNode {
        typ: RelNodeType::Eq,
        children: vec![left.into(), right.into()],
        data: Arc::new(RelAttrType::None),
    }
}

pub fn column_ref_pred(idx: usize) -> RelNode {
    RelNode {
        typ: RelNodeType::ColumnRef,
        children: vec![],
        data: Arc::new(RelAttrType::ColumnRef(idx)),
    }
}

pub fn const_pred(value: i64) -> RelNode {
    RelNode {
        typ: RelNodeType::Const,
        children: vec![],
        data: Arc::new(RelAttrType::Const(value)),
    }
}

pub struct Scan(Arc<RelNode>);

impl Scan {
    pub fn try_from_relnode(node: Arc<RelNode>) -> Option<Self> {
        let RelNodeType::Scan = node.typ else {
            return None;
        };
        Some(Self(node))
    }

    pub fn into_relnode(self) -> Arc<RelNode> {
        self.0
    }

    pub fn table(&self) -> TableId {
        match &*self.0.data {
            RelAttrType::TableId(table) => table.clone(),
            _ => panic!("not a scan node"),
        }
    }
}

pub struct Filter(Arc<RelNode>);

impl Filter {
    pub fn try_from_relnode(node: Arc<RelNode>) -> Option<Self> {
        let RelNodeType::Filter = node.typ else {
            return None;
        };
        Some(Self(node))
    }

    pub fn into_relnode(self) -> Arc<RelNode> {
        self.0
    }

    pub fn child(&self) -> Arc<RelNode> {
        self.0.children[0].clone()
    }

    pub fn cond(&self) -> Arc<RelNode> {
        self.0.children[1].clone()
    }
}

pub struct Join(Arc<RelNode>);

impl Join {
    pub fn try_from_relnode(node: Arc<RelNode>) -> Option<Self> {
        let RelNodeType::Join = node.typ else {
            return None;
        };
        Some(Self(node))
    }

    pub fn into_relnode(self) -> Arc<RelNode> {
        self.0
    }

    pub fn left(&self) -> Arc<RelNode> {
        self.0.children[0].clone()
    }

    pub fn right(&self) -> Arc<RelNode> {
        self.0.children[1].clone()
    }

    pub fn cond(&self) -> Arc<RelNode> {
        self.0.children[2].clone()
    }
}

pub struct Eq(Arc<RelNode>);

impl Eq {
    pub fn try_from_relnode(node: Arc<RelNode>) -> Option<Self> {
        let RelNodeType::Eq = node.typ else {
            return None;
        };
        Some(Self(node))
    }

    pub fn into_relnode(self) -> Arc<RelNode> {
        self.0
    }

    pub fn left(&self) -> Arc<RelNode> {
        self.0.children[0].clone()
    }

    pub fn right(&self) -> Arc<RelNode> {
        self.0.children[1].clone()
    }
}

pub struct ColumnRef(Arc<RelNode>);

impl ColumnRef {
    pub fn try_from_relnode(node: Arc<RelNode>) -> Option<Self> {
        let RelNodeType::ColumnRef = node.typ else {
            return None;
        };
        Some(Self(node))
    }

    pub fn idx(&self) -> usize {
        match &*self.0.data {
            RelAttrType::ColumnRef(idx) => *idx,
            _ => panic!("not a column ref node"),
        }
    }

    pub fn into_relnode(self) -> Arc<RelNode> {
        self.0
    }
}

pub struct Const(Arc<RelNode>);

impl Const {
    pub fn try_from_relnode(node: Arc<RelNode>) -> Option<Self> {
        let RelNodeType::Const = node.typ else {
            return None;
        };
        Some(Self(node))
    }

    pub fn value(&self) -> i64 {
        match &*self.0.data {
            RelAttrType::Const(value) => *value,
            _ => panic!("not a const node"),
        }
    }

    pub fn into_relnode(self) -> Arc<RelNode> {
        self.0
    }
}

pub fn plan() -> RelNode {
    filter(
        join(
            scan(TableId(0)),
            scan(TableId(1)),
            eq_pred(column_ref_pred(1), column_ref_pred(3)),
        ),
        eq_pred(column_ref_pred(2), const_pred(3)),
    )
}

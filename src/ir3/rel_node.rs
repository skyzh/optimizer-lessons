use std::{marker::PhantomData, sync::Arc};

#[derive(Clone)]
pub struct TableId(pub usize);

pub struct Scan<T> {
    pub table: TableId,
    pub _marker: PhantomData<T>,
}

impl<T> Scan<T> {
    pub fn children(&self) -> [&T; 0] {
        []
    }

    pub fn children_mut(&mut self) -> [&mut T; 0] {
        []
    }

    pub fn new(table: TableId) -> Self {
        Self {
            table,
            _marker: PhantomData,
        }
    }
}

pub struct Join<T> {
    pub children: [T; 3],
}

impl<T> Join<T> {
    pub fn children(&self) -> &[T; 3] {
        &self.children
    }

    pub fn children_mut(&mut self) -> &mut [T; 3] {
        &mut self.children
    }

    pub fn new(left: T, right: T, cond: T) -> Self {
        Self {
            children: [left, right, cond],
        }
    }

    pub fn left(&self) -> &T {
        &self.children[0]
    }

    pub fn right(&self) -> &T {
        &self.children[1]
    }

    pub fn cond(&self) -> &T {
        &self.children[2]
    }
}

pub struct Filter<T> {
    pub children: [T; 2],
}

impl<T> Filter<T> {
    pub fn children(&self) -> &[T; 2] {
        &self.children
    }

    pub fn children_mut(&mut self) -> &mut [T; 2] {
        &mut self.children
    }

    pub fn new(child: T, predicate: T) -> Self {
        Self {
            children: [child, predicate],
        }
    }

    pub fn child(&self) -> &T {
        &self.children[0]
    }

    pub fn predicate(&self) -> &T {
        &self.children[1]
    }
}

pub struct EqPred<T> {
    pub children: [T; 2],
}

impl<T> EqPred<T> {
    pub fn children(&self) -> &[T; 2] {
        &self.children
    }

    pub fn children_mut(&mut self) -> &mut [T; 2] {
        &mut self.children
    }

    pub fn new(left: T, right: T) -> Self {
        Self {
            children: [left, right],
        }
    }

    pub fn left(&self) -> &T {
        &self.children[0]
    }

    pub fn right(&self) -> &T {
        &self.children[1]
    }
}

pub struct ColumnRefPred<T> {
    pub column: usize,
    pub children: [T; 0],
}

impl<T> ColumnRefPred<T> {
    pub fn children(&self) -> &[T; 0] {
        &self.children
    }

    pub fn children_mut(&mut self) -> &mut [T; 0] {
        &mut self.children
    }

    pub fn new(column: usize) -> Self {
        Self {
            column,
            children: [],
        }
    }

    pub fn column(&self) -> usize {
        self.column
    }
}

pub struct ConstPred<T> {
    pub value: i64,
    pub children: [T; 0],
}

impl<T> ConstPred<T> {
    pub fn children(&self) -> &[T; 0] {
        &self.children
    }

    pub fn children_mut(&mut self) -> &mut [T; 0] {
        &mut self.children
    }

    pub fn new(value: i64) -> Self {
        Self {
            value,
            children: [],
        }
    }

    pub fn value(&self) -> i64 {
        self.value
    }
}

enum RelNodeInner<T> {
    Scan(Scan<T>),
    Join(Join<T>),
    Filter(Filter<T>),
    Eq(EqPred<T>),
    ColumnRef(ColumnRefPred<T>),
    Const(ConstPred<T>),
}

pub struct GroupId(usize);

pub struct RelMemoNode(RelNodeInner<GroupId>);

pub struct RelNode(RelNodeInner<Arc<Self>>);

pub enum RelBindingNode {
    Node(RelNodeInner<Arc<Self>>),
    Group(GroupId),
}

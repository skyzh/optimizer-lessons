use std::sync::Arc;

struct TableId(usize);

enum RelNodeType {
    Scan(TableId),
    Filter,
    Join,
    Eq,
    ColumnRef(usize),
    Const(i64),
}

struct RelNode {
    typ: RelNodeType,
    children: Vec<Arc<RelNode>>,
}

fn scan(table: TableId) -> RelNode {
    RelNode {
        typ: RelNodeType::Scan(table),
        children: vec![],
    }
}

fn filter(child: RelNode, cond: RelNode) -> RelNode {
    RelNode {
        typ: RelNodeType::Filter,
        children: vec![Arc::new(child), Arc::new(cond)],
    }
}

fn join(left: RelNode, right: RelNode, cond: RelNode) -> RelNode {
    RelNode {
        typ: RelNodeType::Filter,
        children: vec![Arc::new(left), Arc::new(right), Arc::new(cond)],
    }
}

fn eq_pred(left: RelNode, right: RelNode) -> RelNode {
    RelNode {
        typ: RelNodeType::Eq,
        children: vec![Arc::new(left), Arc::new(right)],
    }
}

fn column_ref_pred(idx: usize) -> RelNode {
    RelNode {
        typ: RelNodeType::ColumnRef(idx),
        children: vec![],
    }
}

fn const_pred(value: i64) -> RelNode {
    RelNode {
        typ: RelNodeType::Const(value),
        children: vec![],
    }
}

fn plan() -> RelNode {
    filter(
        join(
            scan(TableId(0)),
            scan(TableId(1)),
            eq_pred(column_ref_pred(1), column_ref_pred(3)),
        ),
        eq_pred(column_ref_pred(2), const_pred(3)),
    )
}

fn join_assoc(node: RelNode) -> Option<RelNode> {
    if let RelNodeType::Join = node.typ {
        if let RelNodeType::Join = node.children[0].typ {

        }
    }
    None
}

mod rel_node;
use std::sync::Arc;

pub use rel_node::*;

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

pub fn join_assoc(node: Arc<RelNode>) -> Option<Arc<RelNode>> {
    if let Some(a) = Join::try_from_relnode(node) {
        if let Some(b) = Join::try_from_relnode(a.left()) {
            return Some(join(b.left(), join(b.right(), a.right(), a.cond()), b.cond()).into());
        }
    }
    None
}

pub fn apply_rule_bottom_up(node: &RelNode, rule: impl Fn(&RelNode) -> Option<RelNode>) -> RelNode {
    let mut children = Vec::new();
    for child in &node.children {
        let child = apply_rule_bottom_up(child, &rule);
        children.push(Arc::new(child));
    }
    RelNode {
        typ: node.typ.clone(),
        children,
    }
}

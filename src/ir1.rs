mod rel_node;
pub use rel_node::*;

use std::sync::Arc;

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

// Rewrite
pub fn join_assoc(node: Arc<RelNode>) -> Option<Arc<RelNode>> {
    if let RelNode::Join(ref a) = &*node {
        if let RelNode::Join(b) = &*a.left {
            return Some(
                join(
                    b.left.clone(),
                    join(b.right.clone(), a.right.clone(), a.cond.clone()),
                    b.cond.clone(),
                )
                .into(),
            );
        }
    }
    None
}

// Recursive access and rewrite
pub fn apply_rule_bottom_up(node: &RelNode, rule: impl Fn(&RelNode) -> Option<RelNode>) -> RelNode {
    let mut children = Vec::new();
    for child in node.children() {
        let child = apply_rule_bottom_up(&child, &rule);
        children.push(Arc::new(child));
    }
    node.clone_with_children(children)
}

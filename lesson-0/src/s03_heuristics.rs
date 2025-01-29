use std::sync::Arc;

use super::*;

// Recursive access and rewrite
pub fn apply_rule_bottom_up(
    node: Arc<RelNode>,
    rule: impl Fn(Arc<RelNode>) -> Option<Arc<RelNode>>,
) -> Arc<RelNode> {
    fn apply_rule_bottom_up_inner(
        node: Arc<RelNode>,
        rule: &impl Fn(Arc<RelNode>) -> Option<Arc<RelNode>>,
    ) -> Arc<RelNode> {
        let mut children = Vec::new();
        for child in node.children() {
            let child = apply_rule_bottom_up_inner(child, rule);
            children.push(child);
        }
        let rel = Arc::new(node.clone_with_children(children));
        rule(rel.clone()).unwrap_or_else(|| rel)
    }
    apply_rule_bottom_up_inner(node, &rule)
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

impl ConstPred {
    pub fn children(&self) -> Vec<Arc<RelNode>> {
        vec![]
    }

    pub fn clone_with_children(&self, children: Vec<Arc<RelNode>>) -> Self {
        let _ = children;
        Self { value: self.value }
    }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bottom_up() {
        let initial = join(
            scan(TableId(0)),
            join(
                scan(TableId(1)),
                scan(TableId(2)),
                eq_pred(column_ref_pred(1), column_ref_pred(3)),
            ),
            eq_pred(column_ref_pred(1), column_ref_pred(3)),
        );
        let expected = join(
            join(
                scan(TableId(2)),
                scan(TableId(1)),
                eq_pred(column_ref_pred(1), column_ref_pred(3)),
            ),
            scan(TableId(0)),
            eq_pred(column_ref_pred(1), column_ref_pred(3)),
        );
        assert_eq!(
            apply_rule_bottom_up(Arc::new(initial), join_commute).as_ref(),
            &expected
        );
    }
}

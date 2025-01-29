use std::sync::Arc;

use super::*;

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

pub fn join_commute(node: Arc<RelNode>) -> Option<Arc<RelNode>> {
    if let RelNode::Join(ref a) = &*node {
        // TODO: rewrite the condition
        return Some(join(a.right.clone(), a.left.clone(), a.cond.clone()).into());
    }
    None
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rule_apply() {
        let initial = join(
            scan(TableId(0)),
            scan(TableId(1)),
            eq_pred(column_ref_pred(1), column_ref_pred(3)),
        );
        let expected = join(
            scan(TableId(1)),
            scan(TableId(0)),
            // obviously, the predicate is wrong... but that's fine for now
            eq_pred(column_ref_pred(1), column_ref_pred(3)),
        );
        assert_eq!(join_commute(Arc::new(initial)).unwrap().as_ref(), &expected);
    }
}

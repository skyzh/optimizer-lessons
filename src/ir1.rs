use std::sync::Arc;

struct TableId(usize);

struct Scan {
    table: TableId,
}

struct Join {
    left: Arc<PlanNode>,
    right: Arc<PlanNode>,
    cond: Arc<Predicate>,
}

struct Filter {
    child: Arc<PlanNode>,
    predicate: Arc<Predicate>,
}

enum PlanNode {
    Scan(Scan),
    Join(Join),
    Filter(Filter),
}

enum Predicate {
    Eq(Arc<Self>, Arc<Self>),
    ColumnRef(usize),
    Const(i64),
}

fn plan() -> PlanNode {
    PlanNode::Filter(Filter {
        child: Arc::new(PlanNode::Join(Join {
            left: Arc::new(PlanNode::Scan(Scan {
                table: TableId(1), /* t1 */
            })),
            right: Arc::new(PlanNode::Scan(Scan {
                table: TableId(2), /* t2 */
            })),
            cond: Arc::new(Predicate::Eq(
                Arc::new(Predicate::ColumnRef(1)), /* t1.y */
                Arc::new(Predicate::ColumnRef(3)), /* t2.y */
            )),
        })),
        predicate: Arc::new(Predicate::Eq(
            Arc::new(Predicate::ColumnRef(2)), /* t1.z */
            Arc::new(Predicate::Const(3)),
        )),
    })
}

// Rewrite
fn join_assoc(node: &PlanNode) -> Option<PlanNode> {
    if let PlanNode::Join(a) = node {
        if let PlanNode::Join(b) = &*a.left {
            return Some(PlanNode::Join(Join {
                left: b.left.clone(),
                right: Arc::new(PlanNode::Join(Join {
                    left: b.right.clone(),
                    right: a.right.clone(),
                    cond: a.cond.clone(),
                })),
                cond: b.cond.clone(),
            }));
        }
    }
    None
}

// Recursive access and rewrite
fn apply_rule_bottom_up(node: &PlanNode, rule: impl Fn(&PlanNode) -> Option<PlanNode>) -> PlanNode {
    let children = Vec::new();
    for child in node.children() {
        if let Some(child) = apply_rule_bottom_up(&child, rule) {
            children.push(child);
        } else {
            children.push(child);
        }
    }
    node.clone_with_children(children)
}

// Alternative representation
struct Memo {}

fn add_to_memo_table(node: &PlanNode) {}

fn match_from_memo_table(node: &PlanNode) {}

// Wildcard match and partially materialized
impl Rule for JoinCommuteRule {}

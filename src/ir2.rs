use std::sync::Arc;

trait PlanNode {}
trait Predicate {}

struct TableId(usize);

enum JoinType {
    Inner,
}

struct Scan {
    table: TableId,
}

impl PlanNode for Scan {}

struct Join {
    left: Arc<dyn PlanNode>,
    right: Arc<dyn PlanNode>,
    cond: Arc<dyn Predicate>,
    join_type: JoinType,
}

impl PlanNode for Join {}

struct Filter {
    child: Arc<dyn PlanNode>,
    predicate: Arc<dyn Predicate>,
}

impl PlanNode for Filter {}

struct EqPred(Arc<dyn Predicate>, Arc<dyn Predicate>);

impl Predicate for EqPred {}

struct ColumnRefPred(usize);

impl Predicate for ColumnRefPred {}

struct ConstPred(i64);

impl Predicate for ConstPred {}

fn plan() -> Arc<dyn PlanNode> {
    Arc::new(Filter {
        child: Arc::new(Join {
            left: Arc::new(Scan {
                table: TableId(1), /* t1 */
            }),
            right: Arc::new(Scan {
                table: TableId(2), /* t2 */
            }),
            cond: Arc::new(EqPred(
                Arc::new(ColumnRefPred(1)), /* t1.y */
                Arc::new(ColumnRefPred(3)), /* t2.y */
            )),
            join_type: JoinType::Inner,
        }),
        predicate: Arc::new(EqPred(
            Arc::new(ColumnRefPred(2)), /* t1.z */
            Arc::new(ConstPred(3)),
        )),
    })
}

package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.Iterator;
import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    private final int afield;
    private final int gfield;
    private final Aggregator.Op op;
    private final TupleDesc tupleDesc;
    private OpIterator[] opIterators = new OpIterator[1];
    private final Aggregator aggregator;
    private OpIterator aggIterator;
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
            this.opIterators[0] = child;
            this.tupleDesc = child.getTupleDesc();
            this.afield = afield;

            if(tupleDesc.getFieldType(afield) == Type.INT_TYPE){
                this.aggregator = new IntegerAggregator(gfield,gfield==-1?null:tupleDesc.getFieldType(gfield),afield,aop);
            }else {
                this.aggregator = new StringAggregator(gfield,gfield==-1?null:tupleDesc.getFieldType(gfield),afield,aop);
            }
            this.gfield = gfield;
            this.op = aop;
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        return this.tupleDesc.getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        return this.tupleDesc.getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        return this.op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        super.open();
        opIterators[0].open();
        while (opIterators[0].hasNext()){
            aggregator.mergeTupleIntoGroup(opIterators[0].next());
        }
        this.aggIterator = aggregator.iterator();
        this.aggIterator.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(aggIterator.hasNext())
            return aggIterator.next();
        else
            return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        super.open();
        this.aggIterator.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
        if(gfield == Aggregator.NO_GROUPING){
            return new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{aggregateFieldName()});
        }else {
            return new TupleDesc(new Type[]{tupleDesc.getFieldType(gfield),Type.INT_TYPE},
                                 new String[]{groupFieldName(),nameOfAggregatorOp(op)+"("+aggregateFieldName()+")"});
        }
    }

    public void close() {
        super.close();
        aggIterator.close();
    }

    @Override
    public OpIterator[] getChildren() {
        return this.opIterators;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.opIterators = children;
    }

}

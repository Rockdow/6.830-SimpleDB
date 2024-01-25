package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * Filter is an operator that implements a relational select.
 */
public class Filter extends Operator {

    private static final long serialVersionUID = 1L;
    private final Predicate predicate;
    private OpIterator[] opIterator = new OpIterator[1];

    /**
     * Constructor accepts a predicate to apply and a child operator to read
     * tuples to filter from.
     * 
     * @param p
     *            The predicate to filter tuples with
     * @param child
     *            The child operator
     */
    public Filter(Predicate p, OpIterator child) {
        this.predicate = p;
        this.opIterator[0] = child;
    }

    public Predicate getPredicate() {
        return this.predicate;
    }

    public TupleDesc getTupleDesc() {
        return opIterator[0].getTupleDesc();
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        super.open();
        opIterator[0].open();
    }

    public void close() {
        super.close();
        opIterator[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        super.open();
        opIterator[0].rewind();
    }

    /**
     * AbstractDbIterator.readNext implementation. Iterates over tuples from the
     * child operator, applying the predicate to them and returning those that
     * pass the predicate (i.e. for which the Predicate.filter() returns true.)
     * 
     * @return The next tuple that passes the filter, or null if there are no
     *         more tuples
     * @see Predicate#filter
     */
    protected Tuple fetchNext() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        Tuple next = null;
        while(opIterator[0].hasNext()){
            next = opIterator[0].next();
            if(next == null) {
                break;
            }
            else {
                if(predicate.filter(next))
                    break;
                else
                    // 如果不匹配predicate的规则，那么要把next置为null，避免返回出错
                    next = null;
            }
        }
        return next;

    }

    @Override
    public OpIterator[] getChildren() {
        return this.opIterator;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.opIterator = children;
    }

}

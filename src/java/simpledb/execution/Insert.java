package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.BufferPool;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    private final TransactionId tid;
    private final int tableId;
    private OpIterator[] opIterators = new OpIterator[1];

    private boolean isCalled;

    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        this.tid = t;
        this.tableId = tableId;
        this.opIterators[0] = child;
        this.isCalled = false;
    }

    // 指的是内容为 插入记录总数的tuple 的 tupleDesc，所以是这样写
    public TupleDesc getTupleDesc() {
        return new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        super.open();
        opIterators[0].open();
    }

    public void close() {
        super.close();
        opIterators[0].close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        super.open();
        opIterators[0].rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(!isCalled){
            isCalled = true;
            int numInserted = 0;
            while(opIterators[0].hasNext()){
                try {
                    Database.getBufferPool().insertTuple(tid,tableId,opIterators[0].next());
                    numInserted++;
                }catch (IOException e){
                    e.printStackTrace();
                    System.exit(0);
                }
            }
            Tuple tuple = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
            tuple.setField(0,new IntField(numInserted));
            return tuple;
        }else {
            return null;
        }
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

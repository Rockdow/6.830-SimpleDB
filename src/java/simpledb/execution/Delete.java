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
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    private final TransactionId tid;
    private OpIterator[] opIterators = new OpIterator[1];
    private boolean isCalled;
    public Delete(TransactionId t, OpIterator child) {
        this.tid = t;
        this.opIterators[0] = child;
        this.isCalled = false;
    }

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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        if(!isCalled){
            isCalled = true;
            int numDeleted = 0;
            while (opIterators[0].hasNext()){
                try {
                    Database.getBufferPool().deleteTuple(tid,opIterators[0].next());
                    numDeleted++;
                } catch (IOException e) {
                    e.printStackTrace();
                    System.exit(0);
                }
            }
            Tuple tuple = new Tuple(new TupleDesc(new Type[]{Type.INT_TYPE}));
            tuple.setField(0,new IntField(numDeleted));
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

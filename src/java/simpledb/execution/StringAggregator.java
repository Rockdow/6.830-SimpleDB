package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

import static simpledb.execution.Aggregator.Op.AVG;
import static simpledb.execution.Aggregator.Op.SUM;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    private final int gbFiled;
    private final Type gbFieldType;
    private final int aField;
    private final Op op;
    private final Map<Field, ArrayList<StringField>> map;
    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        if(what != Op.COUNT)
            throw new IllegalArgumentException("the operation must be count");
        this.gbFiled = gbfield;
        this.aField = afield;
        this.op = what;
        if(gbfield == Aggregator.NO_GROUPING)
            this.gbFieldType = null;
        else
            this.gbFieldType = gbfieldtype;
        map = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(gbFiled == Aggregator.NO_GROUPING){
            StringField noGB = new StringField("NoGB", 4);
            if(map.isEmpty()){
                ArrayList<StringField> stringFields = new ArrayList<>();
                stringFields.add((StringField) tup.getField(aField));
                map.put(noGB,stringFields);
            }else {
                map.get(noGB).add((StringField) tup.getField(aField));
            }
        }else {
            if(!map.containsKey(tup.getField(gbFiled))){
                ArrayList<StringField> stringFields = new ArrayList<>();
                stringFields.add((StringField) tup.getField(aField));
                map.put(tup.getField(gbFiled),stringFields);
            }else {
                ArrayList<StringField> arr = map.get(tup.getField(gbFiled));
                arr.add((StringField) tup.getField(aField));
            }
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        return new OpIterator(){
            private int idx;
            private TupleDesc tupleDesc;
            private ArrayList<Tuple> arr;
            private boolean isOpen = false;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                this.idx = 0;
                arr = new ArrayList<>();
                if(gbFiled == Aggregator.NO_GROUPING){
                    this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE});
                    Tuple tuple = new Tuple(this.tupleDesc);
                    ArrayList<StringField> list = map.get(new StringField("NoGB", 4));
                    if(op == Op.COUNT){
                        tuple.setField(0,new IntField(list.size()));
                    }else {
                        throw new IllegalArgumentException("operation must be count");
                    }
                    this.arr.add(tuple);
                }else{
                    if(gbFieldType == Type.STRING_TYPE)
                        this.tupleDesc = new TupleDesc(new Type[]{Type.STRING_TYPE,Type.INT_TYPE});
                    else
                        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE,Type.INT_TYPE});
                    for(Field field: map.keySet()){
                        ArrayList<StringField> stringFields = map.get(field);
                        Tuple tuple = new Tuple(this.tupleDesc);
                        tuple.setField(0,field);
                        if(op == Op.COUNT){
                            tuple.setField(1,new IntField(stringFields.size()));
                        }else {
                            throw new IllegalArgumentException("operation must be count");
                        }
                        this.arr.add(tuple);
                    }
                }
                this.isOpen = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(!this.isOpen)
                    throw new DbException("not open");
                return this.idx < this.arr.size();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!this.isOpen)
                    throw new DbException("not open");
                else if(this.idx >= this.arr.size())
                    throw new NoSuchElementException();
                return this.arr.get(this.idx++);
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public TupleDesc getTupleDesc() {
                return this.tupleDesc;
            }

            @Override
            public void close() {
                this.isOpen = false;
                this.tupleDesc = null;
                this.idx = 0;
                this.arr = null;
            }
        };
    }

}

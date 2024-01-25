package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;


import java.util.*;

import static simpledb.execution.Aggregator.Op.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */
    // 使用group by排序的列 和 使用聚合函数的列 可能不是同一个
    private final int gbFiled;
    private final Type gbFieldType;
    private final int aField;
    private final Op op;
    private final Map<Field,ArrayList<IntField>> map;


    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
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
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        if(gbFiled == Aggregator.NO_GROUPING){
            StringField noGB = new StringField("NoGB", 4);
            if(map.isEmpty()){
                ArrayList<IntField> intFields = new ArrayList<>();
                intFields.add((IntField) tup.getField(aField));
                map.put(noGB,intFields);
            }else {
                map.get(noGB).add((IntField) tup.getField(aField));
            }
        }else {
            if(!map.containsKey(tup.getField(gbFiled))){
                ArrayList<IntField> intFields = new ArrayList<>();
                intFields.add((IntField) tup.getField(aField));
                map.put(tup.getField(gbFiled),intFields);
            }else {
                ArrayList<IntField> arr = map.get(tup.getField(gbFiled));
                arr.add((IntField) tup.getField(aField));
            }
        }
    }



    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        return new OpIterator() {
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
                    ArrayList<IntField> list = map.get(new StringField("NoGB", 4));
                    tuple.setField(0,list.get(0));
                    this.arr.add(tuple);
                    int sum = list.get(0).getValue();
                    for (int i=1;i< list.size();i++){
                        IntField tupleField = (IntField) tuple.getField(0);
                        IntField listField = list.get(i);
                        switch (op){
                            case MIN:
                                if(tupleField.compare(Predicate.Op.GREATER_THAN,listField))
                                    tuple.setField(0,listField);
                                break;
                            case MAX:
                                if(tupleField.compare(Predicate.Op.LESS_THAN,listField))
                                    tuple.setField(0,listField);
                                break;
                            case AVG:
                            case SUM:
                                sum += listField.getValue();
                                break;
                            case COUNT:
                                break;
                            default:
                                throw new DbException(op+" is unimplemented");
                        }
                    }
                    if(op == SUM){
                        tuple.setField(0,new IntField(sum));
                    }
                    if(op == AVG){
                        tuple.setField(0,new IntField(sum/ list.size()));
                    }
                    if(op == Op.COUNT){
                        tuple.setField(0,new IntField(list.size()));
                    }
                }else{
                    if(gbFieldType == Type.STRING_TYPE)
                        this.tupleDesc = new TupleDesc(new Type[]{Type.STRING_TYPE,Type.INT_TYPE});
                    else
                        this.tupleDesc = new TupleDesc(new Type[]{Type.INT_TYPE,Type.INT_TYPE});
                    for(Field field: map.keySet()){
                        ArrayList<IntField> intFields = map.get(field);
                        Tuple tuple = new Tuple(this.tupleDesc);
                        tuple.setField(0,field);
                        tuple.setField(1, intFields.get(0));
                        this.arr.add(tuple);
                        int sum = intFields.get(0).getValue();
                        for (int i=1;i<intFields.size();i++){
                            IntField intField = intFields.get(i);
                            IntField tupleField = (IntField) tuple.getField(1);
                            switch (op){
                                case MIN:
                                    if(tupleField.compare(Predicate.Op.GREATER_THAN,intField))
                                        tuple.setField(1,intField);
                                    break;
                                case MAX:
                                    if(tupleField.compare(Predicate.Op.LESS_THAN,intField))
                                        tuple.setField(1,intField);
                                    break;
                                case AVG:
                                case SUM:
                                    sum += intField.getValue();
                                    break;
                                case COUNT:
                                    break;
                                default:
                                    throw new DbException(op+" is unimplemented");
                            }
                        }
                        if(op == SUM){
                            tuple.setField(1,new IntField(sum));
                        }
                        if(op == AVG){
                            tuple.setField(1,new IntField(sum/intFields.size()));
                        }
                        if(op == Op.COUNT){
                            tuple.setField(1,new IntField(intFields.size()));
                        }
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

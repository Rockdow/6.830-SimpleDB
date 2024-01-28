package simpledb.optimizer;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.index.BTreeFile;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * TableStats represents statistics (e.g., histograms) about base tables in a
 * query. 
 * 
 * This class is not needed in implementing lab1 and lab2.
 */
public class TableStats {
    private final int  tableTuples;
    private final int tablePages;
    private final int ioCostPerPage;
    private final Map<Integer,IntHistogram> intHistogramMap;
    private final Map<Integer,StringHistogram> strHistogramMap;
    private final TransactionId tid = new TransactionId();

    private static final ConcurrentMap<String, TableStats> statsMap = new ConcurrentHashMap<>();

    static final int IOCOSTPERPAGE = 1000;

    public static TableStats getTableStats(String tablename) {
        return statsMap.get(tablename);
    }

    public static void setTableStats(String tablename, TableStats stats) {
        statsMap.put(tablename, stats);
    }
    
    public static void setStatsMap(Map<String,TableStats> s)
    {
        try {
            java.lang.reflect.Field statsMapF = TableStats.class.getDeclaredField("statsMap");
            statsMapF.setAccessible(true);
            // set的第一个参数为null，因为statsMap是静态变量
            statsMapF.set(null, s);
        } catch (NoSuchFieldException | IllegalAccessException | IllegalArgumentException | SecurityException e) {
            e.printStackTrace();
        }

    }

    public static Map<String, TableStats> getStatsMap() {
        return statsMap;
    }

    public static void computeStatistics() {
        Iterator<Integer> tableIt = Database.getCatalog().tableIdIterator();

        System.out.println("Computing table stats.");
        while (tableIt.hasNext()) {
            int tableid = tableIt.next();
            TableStats s = new TableStats(tableid, IOCOSTPERPAGE);
            setTableStats(Database.getCatalog().getTableName(tableid), s);
        }
        System.out.println("Done.");
    }

    /**
     * Number of bins for the histogram. Feel free to increase this value over
     * 100, though our tests assume that you have at least 100 bins in your
     * histograms.
     */
    static final int NUM_HIST_BINS = 100;

    /**
     * Create a new TableStats object, that keeps track of statistics on each
     * column of a table
     * 
     * @param tableid
     *            The table over which to compute statistics
     * @param ioCostPerPage
     *            The cost per page of IO. This doesn't differentiate between
     *            sequential-scan IO and disk seeks.
     */
    public TableStats(int tableid, int ioCostPerPage) {
        this.ioCostPerPage = ioCostPerPage;
        DbFile databaseFile = Database.getCatalog().getDatabaseFile(tableid);
        TupleDesc tupleDesc = databaseFile.getTupleDesc();
        HashMap<Integer, Integer> max = new HashMap<>();
        HashMap<Integer, Integer> min = new HashMap<>();
        this.intHistogramMap = new HashMap<>();
        this.strHistogramMap = new HashMap<>();
        // 初始化整型字段的值
        for(int i=0;i<tupleDesc.numFields();i++){
            if(tupleDesc.getFieldType(i) == Type.INT_TYPE){
                max.put(i,Integer.MIN_VALUE);
                min.put(i,Integer.MAX_VALUE);
            }
        }
        // 获取各整型字段的最大最小值
        int tupleNum = 0;
        DbFileIterator iterator = databaseFile.iterator(tid);
        try {
            iterator.open();
            while (iterator.hasNext()){
                tupleNum++;
                Tuple next = iterator.next();
                for(int i=0;i<tupleDesc.numFields();i++){
                    if(tupleDesc.getFieldType(i) == Type.INT_TYPE){
                        IntField intField = (IntField) next.getField(i);
                        if(intField.getValue() > max.get(i)){
                            max.put(i,intField.getValue());
                        }
                        if(intField.getValue() < min.get(i)){
                            min.put(i,intField.getValue());
                        }
                    }else if(tupleDesc.getFieldType(i) == Type.STRING_TYPE){
                        StringHistogram strHistogram = new StringHistogram(NUM_HIST_BINS);
                        this.strHistogramMap.put(i,strHistogram);
                    }
                }
            }
            for(Integer integer: max.keySet()){
                IntHistogram intHistogram = new IntHistogram(NUM_HIST_BINS, min.get(integer), max.get(integer));
                this.intHistogramMap.put(integer,intHistogram);
            }
            this.tableTuples = tupleNum;
            iterator.rewind();
            while (iterator.hasNext()){
                Tuple next = iterator.next();
                for(int i=0;i<tupleDesc.numFields();i++){
                    if(tupleDesc.getFieldType(i) == Type.INT_TYPE){
                        IntField intField = (IntField) next.getField(i);
                        this.intHistogramMap.get(i).addValue(intField.getValue());
                    }else if(tupleDesc.getFieldType(i) == Type.STRING_TYPE){
                        StringField strField = (StringField) next.getField(i);
                        this.strHistogramMap.get(i).addValue(strField.getValue());
                    }
                }
            }
            if(databaseFile instanceof HeapFile)
                this.tablePages = ((HeapFile)databaseFile).numPages();
            else if(databaseFile instanceof BTreeFile)
                this.tablePages = ((BTreeFile)databaseFile).numPages();
            else{
                this.tablePages = 0;
                throw new RuntimeException("can not confirm the page");
            }


        } catch (DbException e) {
            throw new RuntimeException(e);
        } catch (TransactionAbortedException e) {
            throw new RuntimeException(e);
        }

    }

    /**
     * Estimates the cost of sequentially scanning the file, given that the cost
     * to read a page is costPerPageIO. You can assume that there are no seeks
     * and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once, so
     * if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page. (Most real hard drives can't
     * efficiently address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */
    public double estimateScanCost() {
        return tablePages * ioCostPerPage;
    }

    /**
     * This method returns the number of tuples in the relation, given that a
     * predicate with selectivity selectivityFactor is applied.
     * 
     * @param selectivityFactor
     *            The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified
     *         selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return (int)(tableTuples * selectivityFactor);
    }

    /**
     * The average selectivity of the field under op.
     * @param field
     *        the index of the field
     * @param op
     *        the operator in the predicate
     * The semantic of the method is that, given the table, and then given a
     * tuple, of which we do not know the value of the field, return the
     * expected selectivity. You may estimate this value from the histograms.
     * */
    public double avgSelectivity(int field, Predicate.Op op) {
        // some code goes here
        return 1.0;
    }

    /**
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the
     * table.
     * 
     * @param field
     *            The field over which the predicate ranges
     * @param op
     *            The logical operation in the predicate
     * @param constant
     *            The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the
     *         predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        if(intHistogramMap.containsKey(field)){
            IntHistogram intHistogram = intHistogramMap.get(field);
            return intHistogram.estimateSelectivity(op,((IntField)constant).getValue());
        }else if(strHistogramMap.containsKey(field)){
            StringHistogram strHistogram = strHistogramMap.get(field);
            return strHistogram.estimateSelectivity(op,((StringField)constant).getValue());
        }else {
            throw new RuntimeException("the field is illegal");
        }
    }

    /**
     * return the total number of tuples in this table
     * */
    public int totalTuples() {
        // some code goes here
        return 0;
    }

}

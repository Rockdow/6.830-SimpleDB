package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
// 这个直方图就是为了计算按某个字段条件过滤后的选择性（selectivity:filter tuples / total tuples）时，不用遍历table
public class IntHistogram {

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */
    private final int[] buckets;
    private final double bucketWidth;
    private final int min;
    private final int max;
    private int totalTuples;

    public IntHistogram(int buckets, int min, int max) {
        this.buckets = new int[buckets];
        // bucketWidth必须保证>=1，以计算==v为例: eqTuples = buckets[idx]*1.0/bucketWidth
        // 如果bucketWidth小于1，那么会得到比实际==v的tuples数量大得多的eqTuples，从而出错
        this.bucketWidth = Math.max((max-min+1)*1.0/buckets,1.0);
        this.min = min;
        this.max = max;
        this.totalTuples = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        if(v>=min && v<max){
            int nthBucket = (int) ((v-min)/bucketWidth);
            buckets[nthBucket]++;
            totalTuples++;
        }
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {
        int idx = (int) ((v-min)/bucketWidth);
        switch (op){
            case EQUALS:
                if(v<min || v>max)
                    return 0.0;
                else {
                    double eqTuples = buckets[idx]*1.0/bucketWidth;
                    return eqTuples/totalTuples;
                }
            case GREATER_THAN:
                if(v<min)
                    return 1.0;
                else if(v>max)
                    return 0.0;
                else {
                    int right = (int) (min+(idx+1)*bucketWidth-1);
                    double sumGtTuples = (right-v)*1.0/bucketWidth*buckets[idx];
                    for(int j=idx+1;j<buckets.length;j++)
                        sumGtTuples+=buckets[j];
                    return sumGtTuples/totalTuples;
                }
            case LESS_THAN:
                if(v<min)
                    return 0.0;
                else if(v>max)
                    return 1.0;
                else {
                    int left = (int) (min+idx*bucketWidth);
                    double sumLtTuples = (v-left)*1.0/bucketWidth*buckets[idx];
                    for(int j=0;j<idx;j++)
                        sumLtTuples+=buckets[j];
                    return sumLtTuples/totalTuples;
                }
            case NOT_EQUALS:
                return 1.0 - estimateSelectivity(Predicate.Op.EQUALS,v);
            case GREATER_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.GREATER_THAN,v-1);
            case LESS_THAN_OR_EQ:
                return estimateSelectivity(Predicate.Op.LESS_THAN,v+1);
            default:
                throw new RuntimeException("illegal operator");
        }
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        double avg = 0.0;
        for (int i=0;i<buckets.length;i++){
            avg += buckets[i]*1.0/totalTuples;
        }
        return avg;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        return "IntHistogram:{" + " buckets:"+buckets.toString()
                                + " bucketWidth:"+bucketWidth
                                + " minVal:"+min
                                + " maxVal:"+max
                                + " totalTuples:"+totalTuples+"}";
    }
}

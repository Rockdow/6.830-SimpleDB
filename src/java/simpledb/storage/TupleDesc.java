package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    private ArrayList<TDItem> tdItems;
    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        Iterator<TDItem> iterator = tdItems.iterator();
        // some code goes here
        return iterator;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        tdItems = new ArrayList<>();
        for(int i=0;i<typeAr.length;i++){
            if(fieldAr[i] == null)
                fieldAr[i] = "col_"+i;
            TDItem tdItem = new TDItem(typeAr[i], fieldAr[i]);
            tdItems.add(tdItem);
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        tdItems = new ArrayList<>();
        for(int i=0;i<typeAr.length;i++){
            TDItem tdItem = new TDItem(typeAr[i], "col_"+i);
            tdItems.add(tdItem);
        }
    }

    public TupleDesc() {
        tdItems = new ArrayList<>();
    }

    public ArrayList<TDItem> getTdItems(){
        return this.tdItems;
    }
    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return tdItems.size();
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if(i<0 || i>= tdItems.size())
            throw new NoSuchElementException();
        TDItem tdItem = tdItems.get(i);
        return tdItem.fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if(i<0 || i>= tdItems.size())
            throw new NoSuchElementException();
        TDItem tdItem = tdItems.get(i);
        return tdItem.fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        if(name == null)
            throw new NoSuchElementException();
        for(int i = 0; i< tdItems.size(); i++){
            TDItem tdItem = tdItems.get(i);
            if(name.equals(tdItem.fieldName)){
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for(int i = 0; i< tdItems.size(); i++){
            TDItem tdItem = tdItems.get(i);
            size += tdItem.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        TupleDesc tupleDesc = new TupleDesc();
        ArrayList<TDItem> tdItems = tupleDesc.getTdItems();
        Iterator<TDItem> iteratorTD1 = td1.iterator();
        Iterator<TDItem> iteratorTD2 = td2.iterator();
        while(iteratorTD1.hasNext()){
            TDItem next = iteratorTD1.next();
            tdItems.add(next);
        }
        while (iteratorTD2.hasNext()){
            TDItem next = iteratorTD2.next();
            tdItems.add(next);
        }
        return tupleDesc;
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        if(o instanceof TupleDesc){
            TupleDesc tupleDesc = (TupleDesc) o;
            if(tupleDesc.numFields() != this.numFields())
                return  false;
            else{
                Iterator<TDItem> iterator1 = tupleDesc.iterator();
                Iterator<TDItem> iterator2 = this.iterator();
                while(iterator1.hasNext() && iterator2.hasNext()){
                    TDItem next1 = iterator1.next();
                    TDItem next2 = iterator2.next();
                    if(!next1.fieldType.equals(next2.fieldType))
                        return  false;
                }
                return true;
            }
        }else {
            return false;
        }
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        final int prime = 31;
        int result = 1;
        result = prime * result + (int) (tdItems.hashCode() ^ (tdItems.hashCode() >>> 32));
        return result;
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        int i = 0;
        StringBuilder s = new StringBuilder();
        for(TDItem tdItem:tdItems){
            Type fieldType = tdItem.fieldType;
            String fieldName = tdItem.fieldName;
            s.append(fieldType.toString()+"["+i+"]"+"("+fieldName+"["+i+"]"+")");
            i++;
            if(i<tdItems.size())
                s.append(",");
        }
        return s.toString();
    }
}

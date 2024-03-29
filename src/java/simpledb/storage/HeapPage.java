package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Catalog;
import simpledb.transaction.TransactionId;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    private final HeapPageId pid;
    private final TupleDesc td;
    private final byte[] header;
    private final Tuple[] tuples;
    private final int numSlots;

    private boolean isDirty;
    private TransactionId dirtyTransId;
    byte[] oldData;
    private final Byte oldDataLock= (byte) 0;

    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;

        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        this.isDirty = false;
        // DataInputStream 用于读取指定类型数据，不能单独使用，必须结合其它流，比如 FileInputStream
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {        
        int tupleSize = td.getSize();
        //向下取整，仅存放完整的tuple
        return (int) Math.floor((BufferPool.getPageSize()*8.0) / (tupleSize * 8.0 + 1));
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // 向上取整，可能会存储未使用的tuple状态信息
        return (int) Math.ceil(getNumTuples()/8.0);
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
        return this.pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (byte b : header) {
            try {
                dos.writeByte(b);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        if(t==null || t.getRecordId()==null || t.getRecordId().getPageId()==null)
            throw new DbException("the tuple is illegal");
        if(!pid.equals(t.getRecordId().getPageId()) || !isSlotUsed(t.getRecordId().getTupleNumber()))
            throw new DbException("the tuple is not in the page or empty");
        markSlotUsed(t.getRecordId().getTupleNumber(),false);
        tuples[t.getRecordId().getTupleNumber()] = null;
    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        if(t==null || t.getTupleDesc()==null)
            throw new DbException("the tuple is illegal");
        if(getNumEmptySlots()==0 || !td.equals(t.getTupleDesc()))
            throw new DbException("the page is full or tupleDesc is mismatch");
        int idx = 0;
        while(idx < tuples.length){
            if(tuples[idx] == null)
                break;
            idx++;
        }
        RecordId recordId = new RecordId(pid, idx);
        t.setRecordId(recordId);
        markSlotUsed(idx,true);
        tuples[idx] = t;
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        if(dirty){
            this.isDirty = true;
            this.dirtyTransId = tid;
        }else {
            this.isDirty = false;
            this.dirtyTransId = null;
        }
    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        if(isDirty)
            return dirtyTransId;
        else
            return null;
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        int num = 0;
        for(int i=0;i<tuples.length;i++){
            if(tuples[i] == null)
                num++;
        }
        return num;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        int nthHeaderByte = i/8;
        int nthBit = i%8;
        // lsb（least significant bits）是每个字节的最右端
        // 不能写成return (header[nthHeaderByte] & (1<<nthBit)) == 1;
        // 因为 相与之后 可能是 000000010，和1的二进制 00000001不同
        return (header[nthHeaderByte] & (1<<nthBit)) != 0;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    // value为true，置为1，value为false，置为0
    private void markSlotUsed(int i, boolean value) {
        int nthHeaderByte = i/8;
        int nthBit = i%8;
        if(value){
            if(!isSlotUsed(i))
                header[nthHeaderByte] = (byte) (header[nthHeaderByte] ^ (1<<nthBit));
        }else {
            if(isSlotUsed(i))
                header[nthHeaderByte] = (byte) (header[nthHeaderByte] ^ (1<<nthBit));
        }

    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        return new Iterator<Tuple>() {
            private int idx = 0;
            private int count = 0;
            private int size = tuples.length - getNumEmptySlots();
            @Override
            public boolean hasNext() {
                return count < size;
            }

            @Override
            public Tuple next() {
                while(tuples[idx]==null)
                    idx++;
                count++;
                // 注意，此处返回后要将idx加1，不然会造成idx无法更新，始终取出同一个tuple
                return tuples[idx++];
            }
        };
    }

}


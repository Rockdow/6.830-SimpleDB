package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    private final File file;
    private final TupleDesc tupleDesc;
    public HeapFile(File f, TupleDesc td){
        // 不能在构造函数中将所有页读入内存，可能会导致内存溢出
        this.file = f;
        this.tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return this.file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // 用文件的hashCode充当唯一ID
        return this.file.hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        return this.tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int tableId = pid.getTableId();
        if(tableId != this.getId())
            throw new RuntimeException("the PageId is not legal");
        int pageNumber = pid.getPageNumber();
        int maxPageNum = this.numPages();
        if(pageNumber<0 || pageNumber>=maxPageNum)
            throw new RuntimeException("the pageNum exceed the limit");
        FileInputStream fis = null;
        BufferedInputStream bis = null;
        try {
            fis = new FileInputStream(file);
            bis = new BufferedInputStream(fis);
            long skipBytes = pageNumber==0?0:pageNumber*BufferPool.getPageSize();
            bis.skip(skipBytes);
            byte[] bytes = new byte[BufferPool.getPageSize()];
            bis.read(bytes);
            bis.close();
            HeapPage heapPage = new HeapPage((HeapPageId) pid, bytes);
            return heapPage;
        }catch (Exception e){
            e.printStackTrace();
            System.exit(0);
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        if(page== null || page.getId()==null || page.getId().getTableId()!=this.getId())
            throw new RuntimeException("the page is not legal");
        int pageNumber = page.getId().getPageNumber();
        if(pageNumber<0 || pageNumber>this.numPages())
            throw new RuntimeException("the pageNum exceed the limit");
        RandomAccessFile raf = null;
        try {
            raf = new RandomAccessFile(file,"rw");
            long skipBytes = pageNumber==0?0:pageNumber*BufferPool.getPageSize();
            raf.seek(skipBytes);
            raf.write(page.getPageData());
            raf.close();
        }catch (IOException e){
            throw e;
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) Math.ceil(this.file.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // 返回的pages会被标记成脏页，留待BufferPool统一刷盘
        ArrayList<Page> pages = new ArrayList<>(1);
        for(int pageNo=0;pageNo<this.numPages();pageNo++){
            HeapPageId pageId = new HeapPageId(getId(), pageNo);
            HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pageId, Permissions.READ_WRITE);
            if(page.getNumEmptySlots() > 0){
                page.insertTuple(t);
                pages.add(page);
                return pages;
            }else{
                //因为未修改页的内容，所以虽然违背了2pl，也可以释放锁
                Database.getBufferPool().unsafeReleasePage(tid,pageId);
            }
        }
        // 走到这步说明已有的page不满足要求，需要创建新页，该新创建的页要立刻写入磁盘（单元测试的意思是这样）
        // 使用空的字节数组创建HeapPage，会划分好header和tuple部分，并初始化为未使用
        HeapPage newPage = new HeapPage(new HeapPageId(getId(), this.numPages()), HeapPage.createEmptyPageData());
        newPage.insertTuple(t);
        this.writePage(newPage);
        // 新建的页加入缓存
        Database.getBufferPool().getPage(tid, newPage.getId(), Permissions.READ_WRITE);
        pages.add(newPage);
        return pages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        ArrayList<Page> pages = new ArrayList<>(1);
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        pages.add(page);
        return pages;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        return new DbFileIterator() {
            private int pageNo = 0;
            private Iterator<Tuple> it = null;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                pageNo = 0;
                if(pageNo < numPages()){
                    HeapPageId heapPageId = new HeapPageId(getId(), pageNo);
                    HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_ONLY);
                    it = heapPage.iterator();
                } else {
                    it = null;
                }

            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(it == null)
                    return false;
                else if(it.hasNext()){
                    return true;
                }else{
                    while(++pageNo<numPages()){
                        HeapPageId heapPageId = new HeapPageId(getId(), pageNo);
                        HeapPage heapPage = (HeapPage)Database.getBufferPool().getPage(tid,heapPageId,Permissions.READ_ONLY);
                        it = heapPage.iterator();
                        if(it.hasNext())
                            return true;
                    }
                    return false;
                }
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(it == null)
                    throw new NoSuchElementException();
                return it.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public void close() {
                pageNo = -1;
                it = null;
            }
        };
    }

}


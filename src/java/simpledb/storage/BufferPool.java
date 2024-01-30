package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.Permissions;
import simpledb.common.DbException;
import simpledb.common.DeadlockException;
import simpledb.transaction.PageLockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private final int pageNum;
    private final Map<PageId,Page> map;
    private final PageLockManager lockManager;
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // 因为静态变量先于类对象存在，所以一般不用this引用
        pageNum = numPages;
        map = new ConcurrentHashMap<>();
        lockManager = new PageLockManager();
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        int lockType = 0;
        if(perm == Permissions.READ_WRITE)
            lockType = 1;
        while(true){
            if(lockManager.acquireLock(tid,pid,lockType))
                break;
        }

        if(map.size() >= pageNum){
            if(map.containsKey(pid)){
                return map.get(pid);
            }else {
                evictPage();
                Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                map.put(pid,page);
                return page;
            }
        }else {
            if(map.containsKey(pid)){
                return map.get(pid);
            }else {
                Page page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);
                map.put(pid,page);
                return page;
            }
        }
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    // 释放page中与事务tid相关的锁，在事务未提交之前调用，违背了两阶段锁原则，所以是unsafe的
    public  void unsafeReleasePage(TransactionId tid, PageId pid) {
        lockManager.releaseLock(tid,pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) {
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
       return lockManager.isHoldLock(tid,p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        List<PageId> list = lockManager.getPageIdWithTID(tid);
        if(commit == true){
            for(PageId pageId:list){
                try {
                    flushPage(pageId);
                    lockManager.releaseLock(tid,pageId);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }else {
            for(PageId pageId:list){
                Page page = Database.getCatalog().getDatabaseFile(pageId.getTableId()).readPage(pageId);
                map.put(pageId,page);
                lockManager.releaseLock(tid,pageId);
            }
        }

    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        List<Page> pages = Database.getCatalog().getDatabaseFile(tableId).insertTuple(tid, t);
        for(Page page:pages){
            page.markDirty(true,tid);
            map.put(page.getId(),page);
        }
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        if(t==null || t.getRecordId()==null || t.getRecordId().getPageId()==null)
            throw new DbException("the tuple is illegal");
        int tableId = t.getRecordId().getPageId().getTableId();
        List<Page> pages = Database.getCatalog().getDatabaseFile(tableId).deleteTuple(tid, t);
        for(Page page:pages){
            page.markDirty(true,tid);
            map.put(page.getId(),page);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        for(PageId pageId:map.keySet()){
            flushPage(pageId);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        Page page = map.get(pid);
        if(page == null)
            throw new RuntimeException("bufferPool do not have the page");
        map.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        Page page = null;
        if(map.containsKey(pid))
            page = map.get(pid);
        else
            page = Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid);

        if(page.isDirty() != null){
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
            page.markDirty(false,null);
            map.put(pid,page);
        }
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
//        PageId evictPageId = null;
//        PageId optPageId = null;
//        int optIdx = new Random().nextInt(map.size());
//        int count = 0;
//        for(PageId pageId:map.keySet()){
//            if(map.get(pageId).isDirty() != null){
//                evictPageId = pageId;
//                break;
//            }else if(count == optIdx){
//                optPageId = pageId;
//            }
//            count++;
//        }
//        if(evictPageId != null){
//            try {
//                flushPage(evictPageId);
//                discardPage(evictPageId);
//            } catch (IOException e) {
//                e.printStackTrace();
//                System.exit(0);
//            }
//        }else {
//            discardPage(optPageId);
//        }

        // 找到一个没有被事务上锁的clean页，将其驱逐
        for(PageId pageId: map.keySet()){
            Page page = map.get(pageId);
            if(page.isDirty() == null){
                if(!lockManager.isExistLock(pageId)){
                    discardPage(pageId);
                    return;
                }
            }
        }
        // 到这说明所有clean页被上锁
        for(PageId pageId: map.keySet()){
            Page page = map.get(pageId);
            if(page.isDirty() == null){
                if(lockManager.isExistLock(pageId)){
                    if(lockManager.isOnlySLock(pageId)) {
                        // 此处违反了2pl，但由于只是读锁，问题不大，因为要把页驱逐，必须把lockManager上对应页的锁也删除
                        lockManager.removePageLock(pageId);
                        discardPage(pageId);
                        return;
                    }
                }else {
                    discardPage(pageId);
                    return;
                }
            }
        }
        // 到这说明都是dirty页
        throw new DbException("the bufferPool is full of dirty page");
    }

}

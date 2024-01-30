package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *参考文档:https://blog.csdn.net/weixin_43414605/article/details/124007676
 *由于ReentrantReadWriteLock是基于线程进行锁处理的，不满足本lab中基于事务进行锁处理的条件，所以需要自己实现一个锁管理类
**/
public class PageLockManager {
    private final ConcurrentHashMap<PageId,ConcurrentHashMap<TransactionId,PageLock>> lockMap;
    private final DependencyGraph dependencyGraph;

    public PageLockManager(){
        this.lockMap = new ConcurrentHashMap<>();
        this.dependencyGraph = new DependencyGraph();
    }

    public synchronized List<PageId> getPageIdWithTID(TransactionId tid){
        ArrayList<PageId> pageIds = new ArrayList<>();
        for(PageId pageId:lockMap.keySet()){
            if(isHoldLock(tid,pageId))
                pageIds.add(pageId);
        }
        return pageIds;
    }
    public synchronized boolean isExistLock(PageId pageId){
        return lockMap.get(pageId)!=null;
    }
    public synchronized boolean isOnlySLock(PageId pageId){
        ConcurrentHashMap<TransactionId, PageLock> tidLockMap = lockMap.get(pageId);
        for(TransactionId tid:tidLockMap.keySet()){
            if(tidLockMap.get(tid).getType() == PageLock.SHARE)
                return true;
        }
        return false;
    }
    public synchronized void removePageLock(PageId pageId){
        lockMap.remove(pageId);
    }

    public synchronized boolean acquireLock(TransactionId tid,PageId pageId,int lockType) throws TransactionAbortedException {
        // 虽然ConcurrentHashMap使用CAS和Volatile实现乐观锁，但多线程之间的并发可能会覆盖对lockMap的修改,所以要加sync锁
        ConcurrentHashMap<TransactionId, PageLock> tidLockMap = lockMap.get(pageId);
        // 页面上没锁
        if(tidLockMap == null){
            PageLock pageLock = new PageLock(tid, pageId, lockType);
            tidLockMap = new ConcurrentHashMap<>();
            tidLockMap.put(tid,pageLock);
            lockMap.put(pageId,tidLockMap);
            return true;
        } else if(tidLockMap.containsKey(tid)){
            // 页面上有该事务的锁
            // 请求读锁，因为页面上该事务的锁要么是写锁要么是读锁，所以直接返回true
            if(lockType == PageLock.SHARE)
                return true;
            // 请求写锁
            else if(lockType == PageLock.EXCLUSIVE) {
                PageLock pageLock = tidLockMap.get(tid);
                // 页面上该事务本身就获取了写锁，直接返回true
                if (pageLock.getType() == PageLock.EXCLUSIVE)
                    return true;
                else {
                    // 此时页面上该事务只有读锁且只有该事务持有该页面上的锁，进行锁升级
                    if (tidLockMap.size() == 1) {
                        pageLock.setType(PageLock.EXCLUSIVE);
                        return true;
                    } else {
                        dependencyGraph.addDependencies(tid,pageId,lockType,lockMap.get(pageId));
                        // 此时有其它事务持有页面上的读锁，和写锁互斥
                        return false;
                    }
                }
            } else {
                throw new RuntimeException("the lockType is illegal");
            }
        } else {
            //页面上没有该事务的锁，所以在size>1时，说明该页面上全是读锁
            if(tidLockMap.size() > 1){
                if(lockType == PageLock.SHARE){
                    PageLock pageLock = new PageLock(tid, pageId, PageLock.SHARE);
                    tidLockMap.put(tid,pageLock);
                    return true;
                }else{
                    dependencyGraph.addDependencies(tid,pageId,lockType,lockMap.get(pageId));
                    return false;
                }

            }else {
                // 页面上的锁只有一个，根据情况返回
                PageLock pageLock = null;
                for(TransactionId transactionId:tidLockMap.keySet())
                    pageLock = tidLockMap.get(transactionId);
                if(pageLock.getType() == PageLock.EXCLUSIVE){
                    dependencyGraph.addDependencies(tid,pageId,lockType,lockMap.get(pageId));
                    return false;
                }
                else {
                    if(lockType == PageLock.EXCLUSIVE){
                        dependencyGraph.addDependencies(tid,pageId,lockType,lockMap.get(pageId));
                        return false;
                    }
                    else {
                        PageLock newPageLock = new PageLock(tid, pageId, PageLock.SHARE);
                        tidLockMap.put(tid,newPageLock);
                        return true;
                    }
                }
            }
        }
    }

    public synchronized boolean isHoldLock(TransactionId tid,PageId pageId){
        ConcurrentHashMap<TransactionId, PageLock> tidLockMap = lockMap.get(pageId);
        if(tidLockMap==null || !tidLockMap.containsKey(tid))
            return false;
        else
            return true;

    }

    public synchronized void releaseLock(TransactionId tid,PageId pageId){
        if(isHoldLock(tid,pageId)){
            ConcurrentHashMap<TransactionId, PageLock> tidLockMap = lockMap.get(pageId);
            tidLockMap.remove(tid);
            if(tidLockMap.size() == 0)
                lockMap.remove(pageId);
            dependencyGraph.removeDependencies(tid,pageId);
        }
    }

}

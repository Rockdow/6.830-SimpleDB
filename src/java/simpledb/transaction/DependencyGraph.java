package simpledb.transaction;

import javafx.util.Pair;
import simpledb.common.Database;
import simpledb.common.DeadlockException;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class DependencyGraph {
    // 记录事务对页的请求边，Pair<PageId, TransactionId>代表了哪个事务持有了对应Page
    private final HashMap<TransactionId, HashSet<Pair<PageId, TransactionId>>> requestEdge;

    public DependencyGraph(){
        this.requestEdge = new HashMap<>();
    }

    private void addDependency(TransactionId requester,TransactionId owner,PageId pid) throws TransactionAbortedException {
        if(requester.equals(owner))
            return;

        //为了死锁检测时，requestEdge能包含所有的Transaction节点，要加入owner
        if(!requestEdge.containsKey(owner))
            requestEdge.put(owner,new HashSet<>());

        if(!requestEdge.containsKey(requester))
            requestEdge.put(requester,new HashSet<>());
        HashSet<Pair<PageId, TransactionId>> pairs = requestEdge.get(requester);
        pairs.add(new Pair<>(pid,owner));
        if(detectDeadLock()){
            // 检测到死锁，那么要将该事务终止，删除所有对该事务的请求边
            Database.getBufferPool().transactionComplete(requester,false);
            requestEdge.remove(requester);// 删除该事务所有的请求
            throw new TransactionAbortedException();
        }
    }
    public synchronized boolean detectDeadLock(){
        // 通过拓扑排序的算法检测环
        HashMap<TransactionId, Integer> indegrees = new HashMap<>();
        for(Map.Entry<TransactionId, HashSet<Pair<PageId, TransactionId>>> entry : requestEdge.entrySet()) {
            TransactionId source = entry.getKey();
            if(!indegrees.containsKey(source))
                indegrees.put(source, 0);

            HashSet<Pair<PageId, TransactionId>> destinations = entry.getValue();
            for(Pair<PageId, TransactionId> p : destinations) {
                TransactionId dest = p.getValue();
                if(!indegrees.containsKey(dest))
                    indegrees.put(dest, 0);
                indegrees.put(dest, indegrees.get(dest)+1);
            }
        }

        Queue<TransactionId> sources = new LinkedList<>();
        for(Map.Entry<TransactionId, Integer> entry : indegrees.entrySet()) {
            int indegree = entry.getValue();
            if(indegree == 0) {
                sources.add(entry.getKey());
            }
        }
        int visitedCount = 0;
        while(sources.size() > 0) {
            TransactionId current = sources.poll();
            visitedCount++;
            HashSet<Pair<PageId, TransactionId>> children = requestEdge.get(current);
            for(Pair<PageId, TransactionId> p : children) {
                TransactionId childId = p.getValue();
                indegrees.put(childId, indegrees.get(childId)-1);
                if(indegrees.get(childId) == 0)
                    sources.add(childId);
            }
        }
        return visitedCount!=requestEdge.size();
    }
    public synchronized void removeDependencies(TransactionId tid,PageId pid){
        Pair<PageId, TransactionId> pair = new Pair<>(pid,tid);
        for(TransactionId transactionId:requestEdge.keySet()){
            HashSet<Pair<PageId, TransactionId>> set = requestEdge.get(transactionId);
            if(set!=null)
                set.remove(pair);
        }
    }
    public synchronized void addDependencies(TransactionId tid, PageId pid, int lockType, ConcurrentHashMap<TransactionId,PageLock> map) throws TransactionAbortedException {
        if(lockType == PageLock.EXCLUSIVE) {
            for(TransactionId other: map.keySet())
                addDependency(tid, other, pid);
        } else {
            for(TransactionId other: map.keySet())
                if(map.get(other).getType() == PageLock.EXCLUSIVE)
                    addDependency(tid, other, pid);
        }
    }
}

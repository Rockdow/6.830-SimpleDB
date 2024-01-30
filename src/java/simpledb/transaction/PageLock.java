package simpledb.transaction;

import simpledb.storage.PageId;

// 因为构造函数的tid不能固定，所以不适合构造为枚举类
public class PageLock {
    public static final int SHARE = 0;
    public static final int EXCLUSIVE = 1;
    private TransactionId tid;
    private PageId pageId;
    private int type;

    public PageLock(TransactionId tid, PageId pageId ,int type){
        this.tid =  tid;
        this.type = type;
        this.pageId = pageId;
    }

    public PageId getPageId() {
        return pageId;
    }

    public TransactionId getTid() {
        return tid;
    }

    public int getType() {
        return type;
    }

    public void setType(int type) {
        this.type = type;
    }
}

package simpledb;

import java.io.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
import java.util.Vector;
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

    /**
     * 定义一个最大页数量
     */
    private int numPages;

    /**
     * 定义一个从PageId到Page的散列表
     */
    private ConcurrentHashMap<PageId,Page> pid2pages;

    private ConcurrentHashMap<PageId,Integer> id2age;
    private int age;
    private ControlForLock controlForLock;


    //为Lab4所写，实现一个锁的类
    private class Lock
    {
        TransactionId tid;
        int lockType;

        public Lock(TransactionId tid,int lockType)
        {
            this.tid = tid;
            this.lockType = lockType;
        }
    }

    //对锁的控制类，以页为粒度进行锁的控制
    private class ControlForLock
    {
        //定义一个哈希，用来映射page和其对应的锁
        ConcurrentHashMap<PageId, Vector<Lock>> id2lock;

        public ControlForLock()
        {
            id2lock = new ConcurrentHashMap<PageId, Vector<Lock>>();
        }

        /**
         * 注意：synchronized是java的关键字，是一种同步锁：一个线程调用方法时，其它线程试图使用该方法的线程将被阻塞
         * 参数：pid，标识是哪个page的锁；
         *      tid标识哪个事务；
         *      LockType标识锁的类型，0：shared clock   1：exclusive clock
         * getLock：返回上锁成功or失败
         * **/
        public synchronized boolean getLock(PageId pid, TransactionId tid, int lockType)
        {
            //如果该页原本就没有锁，则根据事务id和锁的类型新建一个锁给它，并给它新建一个vector
            if (id2lock.get(pid) == null)
            {
                //根据tid和locktype建立一个锁
                Lock lock = new Lock(tid, lockType);
                //由于这个page原本没锁，所以还得建一个vector装它的锁们
                Vector<Lock> locks = new Vector<>();
                locks.add(lock);
                //放到哈希里
                id2lock.put(pid, locks);
                return true;
            }

            //获取这一页原本就有的锁的vector
            Vector<Lock> lockVector = id2lock.get(pid);

            for (Lock lock : lockVector)
            {
                //遍历现有的锁，找到在该事务上已经有的锁，其实对应四种情况：
                /*
                    想要shared，已有ex；
                    想要ex，已有shared；
                    想要ex，已有ex
                    想要shared，已有shared
                 */
                if (lock.tid == tid)
                {

                    //排除掉了后两种情况，此时说明该事务已经有了我们想要的锁，上锁成功
                    if (lock.lockType == lockType)
                        return true;
                    //排除掉了第一种情况，已经有ex锁的事务，不需要再上shared锁
                    if (lock.lockType == 1)
                        return true;
                    //下面的if和else都对应第二种情况：（想要ex，已有shared）
                    //如果该资源现有的锁只有一个，则将其升格为exclusive，上锁成功
                    if (lockVector.size() == 1)
                    {
                        lock.lockType = 1;
                        return true;
                    }
                    //对应第二种情况，现有的是shared类型，且不止一个shared锁，则不能上ex锁，上锁失败
                    else
                    {
                        return false;
                    }
                }
            }

            //剩下没有return的都是这页在这个事务上原本没有加锁的
            //如果这页已有exclusive lock，则不能再与其他锁共存，上锁失败
            if (lockVector.get(0).lockType == 1)
            {
                assert lockVector.size() == 1 : "exclusive lock can't coexist with other locks";
                return false;
            }
            //剩下的是既未在这个事务上有锁，又没有exclusive锁的
            //此时根据你要上锁的类型划分：
            //你要上的锁是shared的，直接加到vector里
            if (lockType == 0)
            {
                Lock lock = new Lock(tid, 0);
                lockVector.add(lock);
                id2lock.put(pid, lockVector);
                return true;
            }
            //这个页本来就有锁，并且你还想上个ex锁，则上锁失败
            return false;
        }

        public synchronized boolean unLock(PageId pid,TransactionId tid){
            //本来就没锁
            assert id2lock.get(pid) != null : "page not locked!";

            //先拿到它的锁列表
            Vector<Lock> lockVector = id2lock.get(pid);
            //遍历锁列表
            for(int i=0;i<lockVector.size();++i)
            {
                //找到tid在该页（pageid）对应的锁，并把它移除
                Lock lock = lockVector.get(i);
                if(lock.tid == tid)
                {
                    lockVector.remove(lock);
                    //如果该页对应的锁只有一把，那么直接把该页对应的锁列表删除
                    if(lockVector.size() == 0)
                        id2lock.remove(pid);
                    return true;
                }
            }
            //如果没有找到该页对应的锁，说明解锁失败
            return false;
        }


        /**
         * 返回这个事务是否对该页上锁的bool值
         **/
        public synchronized boolean holdsLock(PageId pid,TransactionId tid)
        {
            if(id2lock.get(pid) == null)
                return false;
            Vector<Lock> locks = id2lock.get(pid);

            for(Lock lock:locks)
            {
                if(lock.tid == tid)
                {
                    return true;
                }
            }
            return false;
        }
    }


        /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        this.numPages=numPages;
        pid2pages=new ConcurrentHashMap<PageId,Page>(this.numPages);

        //为lab4新增的初始化
        id2age = new ConcurrentHashMap<PageId,Integer>();
        age = 0;
        controlForLock = new ControlForLock();
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
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        //先给该page在事务tid上加锁
        long start = System.currentTimeMillis();//获取开始时间
        long timeout = new Random().nextInt(2000) + 1000;

        boolean isLocked = false;//定义一个bool变量来看是否上好锁了，用于防止死锁
        while(!isLocked)
        {
            long now = System.currentTimeMillis();//获取当前时间

            if(now-start > timeout){
                //如果超时，则判断死锁，事务中断
                throw new TransactionAbortedException();
            }
            //若还未给page上tid的锁，则给他上锁
            if(perm == Permissions.READ_ONLY)
                //shared锁——只读
                isLocked = controlForLock.getLock(pid,tid,0);
            else
                //exclusive锁
                isLocked = controlForLock.getLock(pid,tid,1);
        }


        //如果这个page已经在缓存里，直接返回即可
        if(pid2pages.containsKey(pid)){
            return pid2pages.get(pid);
        }
        //如果这个page不在缓存里，就把page放进缓存里
        else{
            //如果page数已经满了，则要先赶出去一个页面，才能再新加进去
            if(pid2pages.size()>=numPages){
                evictPage();
            }
            //通过pid（PageId）找到这个page
            DbFile dbFile=Database.getCatalog().getDatabaseFile(pid.getTableId());
            Page page=dbFile.readPage(pid);
            //把page存到buffpool里
            pid2pages.put(pid,page);

            //为lab4新增代码：
            id2age.put(pid,age++);
        }
        return pid2pages.get(pid);


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
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        //对该页解锁
        controlForLock.unLock(pid,tid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        //确定提交事务（传参为true）
        transactionComplete(tid,true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        //这个功能已经在锁的控制类里实现了
        return controlForLock.holdsLock(p,tid);
    }


    //为Lab4新增，用于将事务tid对应操作过的page都恢复成未操作的状态
    private synchronized void recover(TransactionId tid) {

        //遍历找tid处理过的page
        for (PageId pid : pid2pages.keySet())
        {
            Page page = pid2pages.get(pid);
            if (page.isDirty() == tid)
            {
                //从磁盘上将该页读取出来
                DbFile file =  Database.getCatalog().getDatabaseFile(pid.getTableId());
                Page pageFromDisk = file.readPage(pid);
                //再把该页从磁盘放到缓冲池里，恢复完成
                pid2pages.put(pid, pageFromDisk);
            }
        }
    }




    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
            throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        if(commit)
        {
            //如果提交了，则把该事务对应的所有页都flush到磁盘（并取消所有页的dirty标记）
            flushPages(tid);
        }
        else
        {
            //不提交则恢复事务tid处理过的页
            recover(tid);
        }

        //为tid事务锁住的页都解锁
        for(PageId pid:pid2pages.keySet())
        {
            if(holdsLock(tid,pid))
                releasePage(tid,pid);
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
        // some code goes here
        // not necessary for lab1
        //根据传入的参数tableId找到对应的file表
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        //在对应的file中插入tuple t，返回被插入数据的page列表pagelist
        ArrayList<Page> pagelist = f.insertTuple(tid,t);
        // 遍历这个page列表，标记脏页并加到cache里
        for (Page p : pagelist)
        {
            p.markDirty(true, tid);
            // adds versions of any pages that have been dirtied to the cache
            // (replacing any existing versions of those pages)
            // so that future requests see up-to-date pages.
            if (pid2pages.size() > numPages)
                evictPage();
            pid2pages.put(p.getId(), p);
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
        // some code goes here
        // not necessary for lab1
        //思路同insert
        DbFile f = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId());
        ArrayList<Page> panellist = f.deleteTuple(tid,t);
        for (Page p : panellist)
        {
            p.markDirty(true, tid);
            if (pid2pages.size() > numPages)
                evictPage();
            pid2pages.put(p.getId(), p);
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        //对缓冲池中存的所有页面flush
        for(Page p:pid2pages.values()) {
            flushPage(p.getId());
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
        // some code goes here
        // not necessary for lab1
        pid2pages.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        //从缓冲池写入磁盘，并取消dirty标识，代表事务真正commit结束
        Page p = pid2pages.get(pid);
        TransactionId tid = null;

        if((tid = p.isDirty())!= null)
        {
            //写日志（事务，更新前，更新后）
            Database.getLogFile().logWrite(tid, p.getBeforeImage(), p);
            Database.getLogFile().force();
            // 将page写到disk里
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
            p.markDirty(false, null);//取消标记脏页
        }

    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        //遍历找到tid事务处理过的所有页，把他们都flush到磁盘里（也就是：从缓冲池写入磁盘，并取消dirty标识，代表事务真正commit结束）
        for (PageId pid : pid2pages.keySet())
        {
            Page page = pid2pages.get(pid);
            if (page.isDirty() == tid)
            {
                flushPage(pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
        //驱逐出bufferpool存的一个page（并且不能是dirty的page）

        assert numPages == pid2pages.size() : "There is no need to evict page";

        //用于寻找并存放最老的pageid及其age
        PageId pageId = null;
        int oldestAge = -1;

        // 寻找age最大的非dirty的page去驱逐出去
        for (PageId pid: id2age.keySet())
        {
            Page page = pid2pages.get(pid);
            if (page.isDirty() != null)//非dirty
                continue;

            if (pageId == null)
            {
                pageId = pid;
                oldestAge = id2age.get(pid);
                continue;
            }

            if (id2age.get(pid) < oldestAge)
            {
                pageId = pid;
                oldestAge = id2age.get(pid);
            }
        }

        //如果没找到：对应着所有page都是dirty的情况
        if (pageId == null)
            throw  new DbException("failed to evict page: all pages are dirty");
        Page page = pid2pages.get(pageId);

        //把找到的这个page从缓冲池中驱逐掉
        pid2pages.remove(pageId);
        id2age.remove(pageId);


    }
}

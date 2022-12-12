package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */

/**
 * After you have implemented HeapPage, you will write methods for HeapFile in this lab to calculate
 * the number of pages in a file and to read a page from the file. You will then be able to fetch tuples from
 * a file stored on disk.
 */
public class HeapFile implements DbFile {
    //java的File类：代表磁盘实际存在的文件和目录
    private File file;
    private TupleDesc tupleDesc;


    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file=f;
        this.tupleDesc=td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
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
        // some code goes here
        //上文提示：We suggest hashing the absolute file name of the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) throws IllegalArgumentException{
        /**
         * DbFile.java的readPage：
         * @throws IllegalArgumentException if the page,xx, does not exist in this file.
         */
        // some code goes here
        byte[] data=new byte[BufferPool.getPageSize()];
        HeapPage heapPage=null;//最后返回的page

        int tableId= pid.getTableId();
        int pgNo=pid.getPageNumber();

        //定义随机访问文件的类对象
        RandomAccessFile randomAccessFile=null;

        try {
            //定义随机访问文件的类对象
            randomAccessFile = new RandomAccessFile(file, "r");
            int pos = pid.getPageNumber() * BufferPool.getPageSize();
            randomAccessFile.seek(pos);
            randomAccessFile.read(data, 0, data.length);
            heapPage=new HeapPage((HeapPageId) pid,data);

        } catch (IOException e) {
            e.printStackTrace();
        }
        return heapPage;
    }

    //从DbFile得到的帮助文档：
    /**
     * Push the specified page to disk.
     *
     * @param page The page to write.  page.getId().pageno() specifies the offset into the file where the page should be written.
     * @throws IOException if the write fails
     *
     */
    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1

        //获取传入的参数page的page num号
        int pgNo = page.getId().getPageNumber();

        // 若这一页的num号大于HeapFile中的page页数，则抛出异常
        if(pgNo > numPages())
        {
            throw new IOException();
        }
        int pgSize = BufferPool.getPageSize();

        //在内存中存page
        RandomAccessFile f = new RandomAccessFile(file,"rw");
        // 在file里找到对应的page位置
        f.seek((long) pgNo *pgSize);

        // 从参数page里面拿出它的存储内容
        byte[] data = page.getPageData();

        //将page里面的数据写到文件里
        f.write(data);
        f.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) (file.length() / BufferPool.getPageSize());
    }

    //从DbFile得到的帮助文档：
    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to add.  This tuple should be updated to reflect that
     *          it is now stored in this file.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        HeapPage page  = null;

        // 遍历整张表的所有数据页，然后判断数据页是否有空slot，有的话调用对应有空slot的page的insertTuple方法去插入页面
        //下面这句话对pgNo遍历，paNo是pageNum，其实就是遍历所有page
        for(int pgNo=0;pgNo<numPages();++pgNo)
        {
            //用读写的方式取出该page，注意传入tid事务参数
            HeapPageId pid = new HeapPageId(getId(),pgNo);
            page = (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
            //若该页有空位置，说明找到了，跳出该遍历循环
            if(page.getNumEmptySlots()!=0)
            {
                break;
            }
            //该页没有空位置，解锁即可
            else
            {
                Database.getBufferPool().releasePage(tid,pid);
            }
        }

        //说明上面的循环都遍历完了，也没找到能有空位置的
        //在磁盘中创建一个空的数据页，再调用HeapPage的insertTuple方法进行插入
        if(page == null || page.getNumEmptySlots() == 0)
        {
            //新建空页
            HeapPageId pid = new HeapPageId(getId(),numPages());
            byte[] data = HeapPage.createEmptyPageData();
            HeapPage heapPage = new HeapPage(pid,data);
            // 将这个新建的HeapPage写到disk
            writePage(heapPage);
            //令page为这个刚刚新建的数据页，与上面找到的有空槽的page一起，表示将要被插入tuple的页
            page = (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);
        }

        //插入我们约定好的page（找到的或新建的）
        page.insertTuple(t);

        //return An ArrayList contain the pages that were modified
        ArrayList<Page> res = new ArrayList<>();
        res.add(page);
        return res;
    }


    //从DbFile得到的帮助文档：
    /**
     * Removes the specified tuple from the file on behalf of the specified
     * transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to delete.  This tuple should be updated to reflect that
     *          it is no longer stored on any page.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be deleted or is not a member
     *   of the file
     */
    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        //找到tuple t对应的Heappage，然后调用对应page的deleteTuple函数

        //找到要被删除的元组所在的page（由record到pid再到page）
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();
        HeapPage page =  (HeapPage)Database.getBufferPool().getPage(tid,pid,Permissions.READ_WRITE);

        //调用对应page的deleteTuple函数
        page.deleteTuple(t);

        //return An ArrayList contain the pages that were modified
        ArrayList<Page> res = new ArrayList<>();
        res.add(page);
        return res;
    }

    // see DbFile.java for javadocs
    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    /**
     * 文档的提示：
     *You will also need to implement the `HeapFile.iterator()` method, which should iterate through
     * the tuples of each page in the HeapFile. The iterator must use the `BufferPool.getPage()` method to
     * access pages in the `HeapFile`. This method loads the page into the buffer pool and will eventually be
     * used (in a later lab) to implement locking-based concurrency control and recovery. Do not load the
     * entire table into memory on the open() call -- this will cause an out of memory error for very large
     * tables.
     */
    //有一些不明白的地方参考了网上的代码，等到写完后面几个lab再回来进一步理解
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }

    private static final class HeapFileIterator implements DbFileIterator{
        private final HeapFile heapFile;
        private final TransactionId tid;
        private Iterator<Tuple> it;
        private int whichPage;

        //构造函数
        public HeapFileIterator(HeapFile file,TransactionId tid){
            this.heapFile = file;
            this.tid = tid;
        }

        //根据DbFileIterator提供给的接口来写函数
        @Override
        public void open() throws DbException, TransactionAbortedException {
            //打开iterator，加载第一页的tuples，令whichpage=0
            whichPage = 0;
            it = getPageTuples(whichPage);
        }

        private Iterator<Tuple> getPageTuples(int pageNumber) throws TransactionAbortedException, DbException{
            // The iterator must use the `BufferPool.getPage()` method to access pages in the `HeapFile`
            if(pageNumber >= 0 && pageNumber < heapFile.numPages()){
                HeapPageId pid = new HeapPageId(heapFile.getId(),pageNumber);
                HeapPage page = (HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
                return page.iterator();
            }else{
                throw new DbException(String.format("heapfile %d does not contain page %d!", pageNumber,heapFile.getId()));
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(it == null){
                return false;
            }

            //当前页tuple遍历完了
            if(!it.hasNext()){
                //看下一页还有没有hasNext
                if(whichPage < (heapFile.numPages()-1)){
                    whichPage++;
                    it = getPageTuples(whichPage);
                    return it.hasNext();
                }
                else{
                    return false;
                }
            }
            //当前页还有tuple未遍历，说明hasNext
            else{
                return true;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        /** @return true if there are more tuples available, false if no more tuples or iterator isn't open. */
            if(it == null || !it.hasNext()){
                throw new NoSuchElementException();
            }
            return it.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            // 重新开一遍
            close();
            open();
        }

        @Override
        public void close() {
            it = null;
        }
    }

}

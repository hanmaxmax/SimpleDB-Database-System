package simpledb;

/** Unique identifier for HeapPage objects. */
public class HeapPageId implements PageId {

    /**
     * 为table里的page创建id结构
     * tableId：page所在的表的id
     * pgNo：page在表中的num
     */
    //为类添加成员属性
    private int tableId;
    private int pgNo;

    /**
     * Constructor. Create a page id structure for a specific page of a
     * specific table.
     *
     * @param tableId The table that is being referenced
     * @param pgNo The page number in that table.
     */
    public HeapPageId(int tableId, int pgNo) {
        // some code goes here
        this.tableId=tableId;
        this.pgNo=pgNo;
    }

    /** @return the table associated with this PageId */
    public int getTableId() {
        // some code goes here
        return tableId;
    }

    /**
     * @return the page number in the table getTableId() associated with
     *   this PageId
     */
    public int getPageNumber() {
        // some code goes here
        return pgNo;
    }

    /**
     * @return a hash code for this page, represented by the concatenation of
     *   the table number and the page number (needed if a PageId is used as a
     *   key in a hash table in the BufferPool, for example.)
     * @see BufferPool
     */
    public int hashCode() {
        // some code goes here
        //利用String类型的的hashcode来创建pageId的hashcode
        String str=""+tableId+pgNo;
        return str.hashCode();
    }

    /**
     * Compares one PageId to another.
     *
     * @param o The object to compare against (must be a PageId)
     * @return true if the objects are equal (e.g., page numbers and table
     *   ids are the same)
     */
    public boolean equals(Object o) {
        // some code goes here
        if (this == o) {
            return true;
        }

        //看o是否为PageId的实例
        if (o instanceof PageId) {
            PageId other = (PageId) o;
            if (other.getPageNumber() == pgNo && other.getTableId() == tableId) {
                return true;
            }
        }
        return false;
    }

        /**
         *  Return a representation of this object as an array of
         *  integers, for writing to disk.  Size of returned array must contain
         *  number of integers that corresponds to number of args to one of the
         *  constructors.
         */
        public int[] serialize () {
            int data[] = new int[2];

            data[0] = getTableId();
            data[1] = getPageNumber();

            return data;
        }

    }
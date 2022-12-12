package simpledb;

import java.io.Serializable;

/**
 * A RecordId is a reference to a specific tuple on a specific page of a
 * specific table.
 */
public class RecordId implements Serializable {
    /**
     * 为page里的tuple创建id结构
     */
    private static final long serialVersionUID = 1L;

    /**
     * 和HeapPageId类似，需要两个成员属性来标注：
     * pid(PageId)：record所在的page
     * tupleno：该record在page的num
     */
    private PageId pid;
    private int tupleno;

    /**
     * Creates a new RecordId referring to the specified PageId and tuple
     * number.
     * 
     * @param pid
     *            the pageid of the page on which the tuple resides
     * @param tupleno
     *            the tuple number within the page.
     */
    public RecordId(PageId pid, int tupleno) {
        // some code goes here
        this.pid=pid;
        this.tupleno=tupleno;
    }

    /**
     * @return the tuple number this RecordId references.
     */
    public int getTupleNumber() {
        // some code goes here
        return tupleno;
    }

    /**
     * @return the page id this RecordId references.
     */
    public PageId getPageId() {
        // some code goes here
        return pid;
    }

    /**
     * Two RecordId objects are considered equal if they represent the same
     * tuple.
     * 
     * @return True if this and o represent the same tuple
     */
    @Override
    public boolean equals(Object o) {
        // some code goes here
        if (this == o) {
            return true;
        }

        //看o是否为PageId的实例
        if (o instanceof RecordId) {
            RecordId other = (RecordId) o;
            if (other.getPageId().equals(pid) && other.getTupleNumber() == tupleno) {
                return true;
            }
        }
        return false;
    }

    /**
     * You should implement the hashCode() so that two equal RecordId instances
     * (with respect to equals()) have the same hashCode().
     * 
     * @return An int that is the same for equal RecordId objects.
     */
    @Override
    public int hashCode() {
        // some code goes here
        //由于要实现两个equal的RecordId的hashcode也相等，所以也可以用字符串：
        //只要pid.getTableId()、pid.getPageNumber()、tupleno相等则hashcode相等
        //因为若pid相等的话，即为pid.getTableId()、pid.getPageNumber()相等
        String str = "" + pid.getTableId()+pid.getPageNumber() + tupleno;
        return str.hashCode();

    }

}

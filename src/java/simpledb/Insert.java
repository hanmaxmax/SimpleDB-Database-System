package simpledb;

import java.io.IOException;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    //根据构造函数，设计下面的属性：
    private TransactionId transactionId;
    private OpIterator child;
    private int tableId;
    //根据下面的函数设计的属性：
    private TupleDesc td;
    private int count;
    private boolean inserted;


    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        //if TupleDesc of child differs from table into which we are to insert.
        if(!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))
        {
            throw new DbException("TupleDesc does not match!");
        }

        this.transactionId=t;
        this.child=child;
        this.tableId=tableId;

        this.td = new TupleDesc(new Type[]{Type.INT_TYPE},new String[]{null});
        this.count = -1; //初始化和关闭时，设为-1
        this.inserted = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        this.count=0; // open和rewind时，count设为0
        this.child.open();
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
        this.count=-1;
        this.inserted=false;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
        this.count=0;
        this.inserted=false;

    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (this.inserted)
            return null;

        this.inserted = true;
        while (this.child.hasNext())
        {
            Tuple t = this.child.next();
            try
            {
                Database.getBufferPool().insertTuple(this.transactionId, this.tableId, t);
                this.count++;
            }
            catch (IOException e)
            {
                e.printStackTrace();
                break;
            }
        }
        Tuple t = new Tuple(this.td);
        t.setField(0, new IntField(this.count));
        return t;

    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {this.child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}

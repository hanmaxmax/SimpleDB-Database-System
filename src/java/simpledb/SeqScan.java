package simpledb;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId transactionId;
    private int tableId;
    private String tableAias;
    private DbFileIterator it;


    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.transactionId=tid;
        this.tableId=tableid;
        this.tableAias=tableAlias;
        this.it=Database.getCatalog().getDatabaseFile(tableid).iterator(tid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return tableAias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableId=tableid;
        this.tableAias=tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        //开启这个迭代器，其实就是开启it=Database.getCatalog().getDatabaseFile(tableid).iterator(tid)迭代器
        it.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        /**
         *理解：这里是为了返回加工后的TupleDesc，
         * 返回TupleDesc，其中包含基础HeapFile中的字段名，前缀为构造函数中的tableAlias字符串。
         * 当连接包含同名字段的表时，此前缀将非常有用。别名和名称应以“.”分隔字符（例如，“别名.字段名”）。
         */
        //获取原来的tupleDesc和field个数
        TupleDesc tupleDesc=Database.getCatalog().getTupleDesc(tableId);
        int fieldNum=tupleDesc.numFields();

        //为新的TupleDesc新建Type[]（fieldType）和String[]（fieldName）
        Type[] types=new Type[fieldNum];
        String[] newNames=new String[fieldNum];

        for (int i=0;i<fieldNum;i++){
            types[i]=tupleDesc.getFieldType(i);//type不变
            String ordName=tupleDesc.getFieldName(i);//把原来的fieldname提取出来
            //设置前缀pre
            String pre="null.";
            if(tableAias!=null){
                pre=tableAias+".";
            }
            newNames[i]=pre+ordName;//新fieldName
        }
        //用新的fieldType和fieldName建立新的TupleDesc
        return new TupleDesc(types,newNames);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return it.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return it.next();
    }

    public void close() {
        // some code goes here
        it.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        it.rewind();
    }
}

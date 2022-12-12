package simpledb;

import java.util.*;
import java.io.*;

/**
 * Each instance of HeapPage stores data for one page of HeapFiles and 
 * implements the Page interface that is used by BufferPool.
 *
 * @see HeapFile
 * @see BufferPool
 *
 */
public class HeapPage implements Page {

    final HeapPageId pid;
    final TupleDesc td;
    final byte header[];
    final Tuple tuples[];
    final int numSlots;

    byte[] oldData;
    private final Byte oldDataLock=new Byte((byte)0);

    //为后面的dirty相关函数添加对应的类属性
    private TransactionId dirtyId; //record that transaction that did the dirtying
    private boolean dirty; //Marks this page as dirty/not dirty


    /**
     * Create a HeapPage from a set of bytes of data read from disk.
     * The format of a HeapPage is a set of header bytes indicating
     * the slots of the page that are in use, some number of tuple slots.
     *  Specifically, the number of tuples is equal to: <p>
     *          floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
     * <p> where tuple size is the size of tuples in this
     * database table, which can be determined via {@link Catalog#getTupleDesc}.
     * The number of 8-bit header words is equal to:
     * <p>
     *      ceiling(no. tuple slots / 8)
     * <p>
     * @see Database#getCatalog
     * @see Catalog#getTupleDesc
     * @see BufferPool#getPageSize()
     */
    public HeapPage(HeapPageId id, byte[] data) throws IOException {
        this.pid = id;
        this.td = Database.getCatalog().getTupleDesc(id.getTableId());
        this.numSlots = getNumTuples();
        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        // allocate and read the header slots of this page
        header = new byte[getHeaderSize()];
        for (int i=0; i<header.length; i++)
            header[i] = dis.readByte();
        
        tuples = new Tuple[numSlots];
        try{
            // allocate and read the actual records of this page
            for (int i=0; i<tuples.length; i++)
                tuples[i] = readNextTuple(dis,i);
        }catch(NoSuchElementException e){
            e.printStackTrace();
        }
        dis.close();

        setBeforeImage();
    }

    /** Retrieve the number of tuples on this page.
        @return the number of tuples on this page
    */
    private int getNumTuples() {        
        // some code goes here
        /**
         * Specifically, the number of tuples is equal to: <p>
         * floor((BufferPool.getPageSize()*8) / (tuple size * 8 + 1))
         */
        int num = (int)Math.floor((BufferPool.getPageSize()*8*1.0)/(td.getSize()*8+1));
        return num;
    }

    /**
     * Computes the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     * @return the number of bytes in the header of a page in a HeapFile with each tuple occupying tupleSize bytes
     */
    private int getHeaderSize() {
        // some code goes here
        /**
         * The number of 8-bit header words is equal to:
         * ceiling(no. tuple slots / 8)
         */
        return (int)Math.ceil(getNumTuples()*1.0/8);
    }
    
    /** Return a view of this page before it was modified
        -- used by recovery */
    public HeapPage getBeforeImage(){
        try {
            byte[] oldDataRef = null;
            synchronized(oldDataLock)
            {
                oldDataRef = oldData;
            }
            return new HeapPage(pid,oldDataRef);
        } catch (IOException e) {
            e.printStackTrace();
            //should never happen -- we parsed it OK before!
            System.exit(1);
        }
        return null;
    }
    
    public void setBeforeImage() {
        synchronized(oldDataLock)
        {
        oldData = getPageData().clone();
        }
    }

    /**
     * @return the PageId associated with this page.
     */
    public HeapPageId getId() {
    // some code goes here
    return pid;
    }

    /**
     * Suck up tuples from the source file.
     */
    private Tuple readNextTuple(DataInputStream dis, int slotId) throws NoSuchElementException {
        // if associated bit is not set, read forward to the next tuple, and
        // return null.
        if (!isSlotUsed(slotId)) {
            for (int i=0; i<td.getSize(); i++) {
                try {
                    dis.readByte();
                } catch (IOException e) {
                    throw new NoSuchElementException("error reading empty tuple");
                }
            }
            return null;
        }

        // read fields in the tuple
        Tuple t = new Tuple(td);
        RecordId rid = new RecordId(pid, slotId);
        t.setRecordId(rid);
        try {
            for (int j=0; j<td.numFields(); j++) {
                Field f = td.getFieldType(j).parse(dis);
                t.setField(j, f);
            }
        } catch (java.text.ParseException e) {
            e.printStackTrace();
            throw new NoSuchElementException("parsing error!");
        }

        return t;
    }

    /**
     * Generates a byte array representing the contents of this page.
     * Used to serialize this page to disk.
     * <p>
     * The invariant here is that it should be possible to pass the byte
     * array generated by getPageData to the HeapPage constructor and
     * have it produce an identical HeapPage object.
     *
     * @see #HeapPage
     * @return A byte array correspond to the bytes of this page.
     */
    public byte[] getPageData() {
        int len = BufferPool.getPageSize();
        ByteArrayOutputStream baos = new ByteArrayOutputStream(len);
        DataOutputStream dos = new DataOutputStream(baos);

        // create the header of the page
        for (int i=0; i<header.length; i++) {
            try {
                dos.writeByte(header[i]);
            } catch (IOException e) {
                // this really shouldn't happen
                e.printStackTrace();
            }
        }

        // create the tuples
        for (int i=0; i<tuples.length; i++) {

            // empty slot
            if (!isSlotUsed(i)) {
                for (int j=0; j<td.getSize(); j++) {
                    try {
                        dos.writeByte(0);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                }
                continue;
            }

            // non-empty slot
            for (int j=0; j<td.numFields(); j++) {
                Field f = tuples[i].getField(j);
                try {
                    f.serialize(dos);
                
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        // padding
        int zerolen = BufferPool.getPageSize() - (header.length + td.getSize() * tuples.length); //- numSlots * td.getSize();
        byte[] zeroes = new byte[zerolen];
        try {
            dos.write(zeroes, 0, zerolen);
        } catch (IOException e) {
            e.printStackTrace();
        }

        try {
            dos.flush();
        } catch (IOException e) {
            e.printStackTrace();
        }

        return baos.toByteArray();
    }

    /**
     * Static method to generate a byte array corresponding to an empty
     * HeapPage.
     * Used to add new, empty pages to the file. Passing the results of
     * this method to the HeapPage constructor will create a HeapPage with
     * no valid tuples in it.
     *
     * @return The returned ByteArray.
     */
    public static byte[] createEmptyPageData() {
        int len = BufferPool.getPageSize();
        return new byte[len]; //all 0
    }

    /**
     * Delete the specified tuple from the page; the corresponding header bit should be updated to reflect
     *   that it is no longer stored on any page.
     * @throws DbException if this tuple is not on this page, or tuple slot is
     *         already empty.
     * @param t The tuple to delete
     */
    public void deleteTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1
        assert t != null;
        RecordId recordId = t.getRecordId();
        if (recordId != null && pid.equals(recordId.getPageId()))
        {
            for (int i = 0; i < numSlots; i++)
            {
                if (isSlotUsed(i) && t.getRecordId().equals(tuples[i].getRecordId()))
                {
                    markSlotUsed(i, false);
                    tuples[i] = null;
                    return;
                }
            }
            throw new DbException("deleteTuple: Error: tuple slot is empty");
        }
        throw new DbException("deleteTuple: Error: tuple is not on this page");


//        //下面这一版的错误在于没有比较pid与tuple所在的pageId是否一致
//
//        // 先根据传进来的tuple元组找到要进行操作的元组的num，然后在page存的tuple[]种找到它
//        int tupleNumber = t.getRecordId().getTupleNumber();
//
//        //throws DbException if this tuple is not on this page, or tuple slot is already empty
//        if(tuples[tupleNumber] == null){
//            throw new DbException("tuple does not exist");
//        }
//        if(!isSlotUsed(tupleNumber)){
//            throw new DbException("the slot is already empty");
//        }
//
//        //改正的错误——————————————————————————————————————————
//        if(!pid.equals(t.getRecordId().getPageId())){
//            throw new DbException("wrong");
//        }
//        //改正的错误——————————————————————————————————————————
//
//
////        if(!t.equals(tuples[tupleNumber]))
////        {
////           throw new DbException("tuple does not exits");
////        }
//
//        else
//        {
//            markSlotUsed(tupleNumber,false);
//            tuples[tupleNumber] = null;
//        }

    }

    /**
     * Adds the specified tuple to the page;  the tuple should be updated to reflect
     *  that it is now stored on this page.
     * @throws DbException if the page is full (no empty slots) or tupledesc
     *         is mismatch.
     * @param t The tuple to add.
     */
    public void insertTuple(Tuple t) throws DbException {
        // some code goes here
        // not necessary for lab1

        //throws DbException if the page is full (no empty slots) or tupledesc is mismatch.
        if(getNumEmptySlots() == 0 || !t.getTupleDesc().equals(td))
        {
            throw new DbException("page is full or tuple descriptor does not match");
        }

        //若没满，则循环遍历寻找空位置，添加进去
        for(int i=0;i<numSlots;++i)
        {
            if(!isSlotUsed(i))
            {
                markSlotUsed(i,true);
                t.setRecordId(new RecordId(pid,i));
                tuples[i] = t;
                break;
            }
        }
    }

    /**
     * Marks this page as dirty/not dirty and record that transaction
     * that did the dirtying
     */
    public void markDirty(boolean dirty, TransactionId tid) {
        // some code goes here
	// not necessary for lab1
        this.dirty  = dirty;
        this.dirtyId = tid;

    }

    /**
     * Returns the tid of the transaction that last dirtied this page, or null if the page is not dirty
     */
    public TransactionId isDirty() {
        // some code goes here
	// Not necessary for lab1
        if(this.dirty)
            return this.dirtyId;
        return null;      
    }

    /**
     * Returns the number of empty slots on this page.
     */
    public int getNumEmptySlots() {
        // some code goes here
        int num=0;
        for(int i=0;i<numSlots;i++){
            if(!isSlotUsed(i)){
                num++;
            }
        }
        return num;
    }

    /**
     * Returns true if associated slot on this page is filled.
     */
    public boolean isSlotUsed(int i) {
        // some code goes here
        //由于header里面存了每个tuple slot的bitmap，根据bitmap里面的0和1，可以判断tuple是否有效
        int num = i/8;//计算这个bit在第几个字节
        int mod = i%8;//计算比特在该字节的第几位（JVM使用big-ending）从右往左算
        int bitidx=header[num];
        int bit=(bitidx>>mod)&1;//将该比特移到它所在字节的最右边，使其所在字节变成[xxxxxxx1]或者[xxxxxxx0]，然后与[00000001]做与运算，相当于[xxxxxxx1]和[00000001]做与运算，看结果是不是1
        return bit==1;
    }

    /**
     * Abstraction to fill or clear a slot on this page.
     */
    private void markSlotUsed(int i, boolean value) {
        // some code goes here
        // not necessary for lab1

        //首先找到下标为i的slot
        byte b = header[Math.floorDiv(i,8)]; //8位一字节的寻找
        byte mask = (byte)(1<<(i%8)); //mask：把要更改的那一位置1，其余置0

        //由于只能对字节更改：
        if(value)
        {
            // 0或x = x，1或x = 1，实现要更改的那一位置1的功能
            header[Math.floorDiv(i,8)] = (byte) (b|mask);
        }
        else
        {
            // 0与x = 0，1与x = x，实现要更改的那一位置0的功能
            header[Math.floorDiv(i,8)] = (byte) (b&(~mask));
        }
    }

    /**
     * @return an iterator over all tuples on this page (calling remove on this iterator throws an UnsupportedOperationException)
     * (note that this iterator shouldn't return tuples in empty slots!)
     */
    public Iterator<Tuple> iterator() {
        // some code goes here
        List<Tuple> filledTuples = new ArrayList<Tuple>();
        for(int i=0;i<numSlots;++i){
            if(isSlotUsed(i)){
                filledTuples.add(tuples[i]);
            }

        }
        return filledTuples.iterator();
    }

}


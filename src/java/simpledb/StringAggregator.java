package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    // 与IntegerAggregator类似
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;

    private Map<Field, Integer> groupMap;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        // 与IntegerAggregator不同的是，what只能为‘COUNT’，否则throw错误
        if (!what.equals(Op.COUNT))
            throw new IllegalArgumentException("Only COUNT is supported for String fields!");
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield=afield;
        this.what=what;
        this.groupMap = new HashMap<>();

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        //a_field: 从给定的tuple中用下标afield找到聚合列
        StringField a_field = (StringField) tup.getField(this.afield);

        //gb_field: 从给定的tuple中用下标gb_field找到分组列
        Field gb_field;
        if (this.gbfield==NO_GROUPING)
            gb_field=null;
        else
            gb_field=tup.getField(this.gbfield);

        //与IntegerAggregator的COUNT类似
        if (gb_field != null && gb_field.getType() != this.gbfieldtype) {
            throw new IllegalArgumentException("Given tuple has wrong type");
        }
        if (!this.groupMap.containsKey(gb_field))
            this.groupMap.put(gb_field, 1);
        else
            this.groupMap.put(gb_field, this.groupMap.get(gb_field) + 1);

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new AggregateIterator(this.groupMap, this.gbfieldtype);
    }



    //写一个基类的聚合的迭代器，可以给string和int聚合的迭代器继承/接口
    static class AggregateIterator implements OpIterator {

        protected Iterator<Map.Entry<Field, Integer>> it;
        TupleDesc td;

        private Map<Field, Integer> groupMap;
        protected Type itgbfieldtype;

        public AggregateIterator(Map<Field, Integer> groupMap, Type gbfieldtype) {
            this.groupMap = groupMap;
            this.itgbfieldtype = gbfieldtype;

            if (this.itgbfieldtype == null)
                this.td = new TupleDesc(new Type[]{Type.INT_TYPE}, new String[]{"aggregateVal"});
            else
                this.td = new TupleDesc(new Type[]{this.itgbfieldtype, Type.INT_TYPE}, new String[]{"groupVal", "aggregateVal"});
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            this.it = groupMap.entrySet().iterator();
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            return it.hasNext();
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Map.Entry<Field, Integer> entry = this.it.next();
            Field f = entry.getKey();
            Tuple rtn = new Tuple(this.td);
            //setFields
            if (f == null)
            {
                rtn.setField(0, new IntField(entry.getValue()));
            }
            else
            {
                rtn.setField(0, f);
                rtn.setField(1, new IntField(entry.getValue()));
            }
            return rtn;
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            this.it = groupMap.entrySet().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return this.td;
        }

        @Override
        public void close() {
            this.it = null;
            this.td = null;
        }


    }
}

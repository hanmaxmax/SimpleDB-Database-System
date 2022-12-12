package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    //根据构造函数确定类的属性
    private int gbfield; //该索引值指定了要使用tuple的哪一个列来分组
    private Type gbfieldtype; //指定了作为分组依据的那一列的值的类型
    private int afield; //该索引指定了要使用tuple的哪一个列来聚合
    private Op what; //聚合操作符
    //根据Aggregator.java：what包含——MIN, MAX, SUM, AVG, COUNT
    //SUM_COUNT and SC_AVG只在lab7使用

    // ( COUNT , SUM , AVG , MIN , MAX )
    //由于做avg运算时，要做除法，所以先存到list里，等聚合完之后再算
    private Map<Field, Integer> groupMap;
    private Map<Field, List<Integer>> avgMap;

    //给SC_AVG使用
    private Map<Field,Integer> countMap;


    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupMap = new HashMap<>();
        this.avgMap = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here

        //a_field: 从给定的tuple中用下标afield找到聚合列，并返回为intfield类型
        IntField a_field = (IntField) tup.getField(this.afield);
        //gb_field: 从给定的tuple中用下标gb_field找到分组列
        Field gb_field;

        if (this.gbfield == NO_GROUPING)
            gb_field = null;
        else
            gb_field = tup.getField(this.gbfield);

        int value = a_field.getValue();

        //检测给出的分组列和分组列的类型是否一致，防止报错
        if (gb_field != null && gb_field.getType() != this.gbfieldtype) {
            throw new IllegalArgumentException("Wrong type! ");
        }

        // 开始聚合操作——MIN, MAX, SUM, AVG, COUNT
        switch (this.what) {
            case MIN:
                //若map不包含gb_field的key，则直接把参数tuple的a_field那个field值放进去
                if (!this.groupMap.containsKey(gb_field))
                    this.groupMap.put(gb_field, value);
                else
                    //若这个map已经包含这个键了，则将原来存的最小值与参数tuple的a_field那个field值比较取最小
                    this.groupMap.put(gb_field, Math.min(this.groupMap.get(gb_field), value));
                break;

            case MAX:
                //逻辑同min
                if (!this.groupMap.containsKey(gb_field))
                    this.groupMap.put(gb_field, value);
                else
                    this.groupMap.put(gb_field, Math.max(this.groupMap.get(gb_field), value));
                break;

            case SUM:
                if (!this.groupMap.containsKey(gb_field))
                    this.groupMap.put(gb_field, value);
                else
                    //加到原来存的值上
                    this.groupMap.put(gb_field, this.groupMap.get(gb_field) + value);
                break;


            case AVG:
                //求AVG不能直接进行运算，因为整数的除法是不精确的，所以需要把所有字段值用个list保存起来，当需要获取聚合结果时，再进行计算返回
                if (!this.avgMap.containsKey(gb_field)) {
                    //若map不包含gb_field的key，则新建一个list，把参数tuple的a_field索引下的field值放进去
                    List<Integer> list = new ArrayList<>();
                    list.add(value);
                    this.avgMap.put(gb_field, list);
                } else {
                    //否则直接加进去就行
                    List<Integer> list = this.avgMap.get(gb_field);
                    list.add(value);
                }
                break;

            case COUNT:
                //原理同min、max等
                if (!this.groupMap.containsKey(gb_field))
                    this.groupMap.put(gb_field, 1);
                else {
                    this.groupMap.put(gb_field, this.groupMap.get(gb_field) + 1);
                }
                break;
            case SUM_COUNT:
                throw new IllegalArgumentException("NotImplemented!");
            case SC_AVG:
                throw new IllegalArgumentException("NotImplemented!");

            default:
                throw new IllegalArgumentException("Aggregate not supported!");
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new IntAggIterator();
    }

    private class IntAggIterator extends StringAggregator.AggregateIterator {

        //AggregateIterator已经写了protected Iterator<Map.Entry<Field, Integer>> it
        //所以IntAggIterator只需要再定义一个avg（映射list）的iterator即可
        private Iterator<Map.Entry<Field, List<Integer>>> avgIt;
        private boolean isAvg;
        // 先把SUM_COUNT和SC_AVG的情况写出来
        private boolean isSCAvg;
        private boolean isSumCount;

        IntAggIterator() {
            super(groupMap, gbfieldtype);
            //判断是操作符否为AVG
            this.isAvg = what.equals(Op.AVG);

            // 先把SUM_COUNT和SC_AVG的情况写出来
            this.isSCAvg = what.equals(Op.SC_AVG);
            this.isSumCount = what.equals(Op.SUM_COUNT);

            if (isSumCount)
            {
                this.td = new TupleDesc(new Type[] {this.itgbfieldtype, Type.INT_TYPE, Type.INT_TYPE},
                        new String[] {"groupVal", "sumVal", "countVal"});
            }
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            super.open();
            if (this.isAvg || this.isSumCount)
                this.avgIt = avgMap.entrySet().iterator();
            else
            {
                this.avgIt = null;
            }
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if (this.isAvg || this.isSumCount)
                return avgIt.hasNext();
            return super.hasNext();
        }

        void setFields(Tuple rtn, int value, Field f) {
            if (f == null) {
                rtn.setField(0, new IntField(value));
            }
            else {
                rtn.setField(0, f);
                rtn.setField(1, new IntField(value));
            }
        }

        //求list里存的数之和
        private int sumList(List<Integer> l) {
            int sum = 0;
            for (int i : l)
                sum += i;
            return sum;
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            Tuple rtn = new Tuple(td);
            if (this.isAvg || this.isSumCount)
            {
                //avgOrSCEntry存放Map<Field, List<Integer>>的键值对
                Map.Entry<Field, List<Integer>> avgOrSCEntry = this.avgIt.next();
                Field avgOrSCField = avgOrSCEntry.getKey(); // 键
                List<Integer> avgOrSCList = avgOrSCEntry.getValue(); // 值
                if (this.isAvg)
                {
                    //Avg的计算方法：sumList(avgOrSCList) / avgOrSCList.size()
                    int value = this.sumList(avgOrSCList) / avgOrSCList.size();
                    this.setFields(rtn, value, avgOrSCField); //将值存在tuple的field里
                    return rtn;
                }
                else
                {
                    //SumCount的计算
                    this.setFields(rtn, sumList(avgOrSCList), avgOrSCField);
                    if (avgOrSCField != null)
                        rtn.setField(2, new IntField(avgOrSCList.size()));
                    else
                        rtn.setField(1, new IntField(avgOrSCList.size()));
                    return rtn;
                }
            }
            else if (this.isSCAvg)
            {
                Map.Entry<Field, Integer> entry = this.it.next();
                Field f = entry.getKey();
                this.setFields(rtn, entry.getValue() / countMap.get(f), f);
                return rtn;
            }
            //讨论完int里面特有的运算符之后，剩下的就返回它的基类所定义过的运算符的计算方法
            return super.next();
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            super.rewind();
            if (this.isAvg || this.isSumCount)
                this.avgIt = avgMap.entrySet().iterator();
        }

        @Override
        public TupleDesc getTupleDesc() {
            return super.getTupleDesc();
        }

        @Override
        public void close() {
            super.close();
            this.avgIt = null;
        }

    }


}





package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;

    // 根据构造函数定义类的属性
    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;

    private Aggregator aggregator;
    private OpIterator it;
    private TupleDesc td;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        this.child = child; //用于不断提供tuples
        this.afield = afield; //标识我们需要聚合的列
        this.gfield = gfield; //标识结果中我们需要group by 的列
        this.aop = aop; //我们需要使用的Aggregation operator


        //child的group by列的列类型
        Type gfieldtype = gfield == -1 ? null : this.child.getTupleDesc().getFieldType(this.gfield);

        if(this.child.getTupleDesc().getFieldType(this.afield) == (Type.STRING_TYPE))
        {
            this.aggregator = new StringAggregator(this.gfield,gfieldtype,this.afield,this.aop);
        }
        else
        {
            this.aggregator = new IntegerAggregator(this.gfield,gfieldtype,this.afield,this.aop);
        }
        this.it = this.aggregator.iterator();

        //下面生成TupleDesc td
        List<Type> types = new ArrayList<>();
        List<String> names = new ArrayList<>();

        if (gfieldtype != null)
        {
            types.add(gfieldtype);//添加：child的group by列的列类型
            names.add(this.child.getTupleDesc().getFieldName(this.gfield)); //添加：child的group by列的列名称
        }
        types.add(this.child.getTupleDesc().getFieldType(this.afield)); //添加：child的聚合列的列类型
        names.add(this.child.getTupleDesc().getFieldName(this.afield));//添加：child的聚合列的列名称
        if (aop.equals(Aggregator.Op.SUM_COUNT))
        {
            types.add(Type.INT_TYPE);
            names.add("COUNT");
        }
        assert (types.size() == names.size());//断言——这两个的大小一样
        //Aggregate的tupleDesc
        this.td = new TupleDesc(types.toArray(new Type[types.size()]), names.toArray(new String[names.size()]));

    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	return this.gfield;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
	return this.td.getFieldName(0);
    //根据构造函数中对td的定义，其实td.getFieldName(0)就是child.getTupleDesc().getFieldName(gfield);
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
        //根据构造函数中对td的定义，若没有gfield，则aggregateFieldName是td的fieldname[0]，若有，则是[1]
        if(this.gfield == -1)

            return this.td.getFieldName(0);
        else
            return this.td.getFieldName(1);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
        this.child.open();
        while (this.child.hasNext())
            this.aggregator.mergeTupleIntoGroup(this.child.next());
        this.it.open();
        super.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        while (this.it.hasNext())
            return this.it.next();
        return null;

    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        this.child.rewind();
        this.it.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
	return this.td;
    }

    public void close() {
	// some code goes here
        super.close();
        this.child.close();
        this.it.close();
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

        //改变child之后更新tupeDesc td（代码跟构造函数一样）
        List<Type> types = new ArrayList<>();
        List<String> names = new ArrayList<>();
        Type gfieldtype = gfield == -1 ? null : this.child.getTupleDesc().getFieldType(this.gfield);
        // group field
        if (gfieldtype != null)
        {
            types.add(gfieldtype);
            names.add(this.child.getTupleDesc().getFieldName(this.gfield));
        }
        types.add(this.child.getTupleDesc().getFieldType(this.afield));
        names.add(this.child.getTupleDesc().getFieldName(this.afield));
        if (aop.equals(Aggregator.Op.SUM_COUNT))
        {
            types.add(Type.INT_TYPE);
            names.add("COUNT");
        }
        assert (types.size() == names.size());
        this.td = new TupleDesc(types.toArray(new Type[types.size()]), names.toArray(new String[names.size()]));

    }
    
}

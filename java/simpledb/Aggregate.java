package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private Aggregator.Op op;
    private DbIterator child;
    private TupleDesc td;
    private int aggField;
    private int grpField = -1;
    private Aggregator agg;
    private TupleIterator tupIter=null;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The DbIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(DbIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        this.child = child;
        this.aggField = afield;
        this.grpField = gfield;

        Type gtype;
        gtype = (gfield == -1 ? null : child.getTupleDesc().getFieldType(gfield));

        this.op = aop;

        // depending on aggregate column, create aggregator constructor
        switch (child.getTupleDesc().getFieldType(afield)){
            case INT_TYPE:
                agg = new IntegerAggregator(grpField,gtype,aggField, op);
                break;
            case STRING_TYPE:
                agg = new StringAggregator(grpField,gtype,aggField, op);
                break;
            default:
                throw new IllegalArgumentException("Invalid Field Type! ");
        }
        this.td = getTupleDesc();
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
	return (grpField == -1 ? Aggregator.NO_GROUPING : grpField);
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
	    if (grpField != -1)
            return child.getTupleDesc().getFieldName(grpField);
        return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
	return aggField;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
	return child.getTupleDesc().getFieldName(aggField);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
	return op;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
        child.open();
        super.open();
        while (child.hasNext()) {
            Tuple tup = child.next();
            agg.mergeTupleIntoGroup(tup);
        }
        tupIter = (TupleIterator) agg.iterator();
        tupIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate, If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        if (tupIter.hasNext())
            return tupIter.next();
        else
            return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        child.rewind();
        tupIter = (TupleIterator) agg.iterator();
        tupIter.open();
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
        Type[] typeArr = null;
        String[] fieldArr = null;

        if (grpField == -1) {
            typeArr = new Type[1];
            fieldArr = new String[1];
            typeArr[0] = child.getTupleDesc().getFieldType(aggField);
            fieldArr[0] = nameOfAggregatorOp(op) + child.getTupleDesc().getFieldName(aggField);
        }
        else // two-field tuple
        {
            typeArr = new Type[2];
            fieldArr = new String[2];
            typeArr[0] = child.getTupleDesc().getFieldType(grpField);
            fieldArr[0] = "GroupBy" + child.getTupleDesc().getFieldName(grpField);
            typeArr[1] = child.getTupleDesc().getFieldType(aggField);
            fieldArr[1] = nameOfAggregatorOp(op) + child.getTupleDesc().getFieldName(aggField);
        }
        return new TupleDesc(typeArr, fieldArr);
    }

    public void close() {
	// some code goes here
        child.close();
        if (tupIter != null)
            tupIter.close();
        super.close();
    }

    @Override
    public DbIterator[] getChildren() {
	// some code goes here
        DbIterator[] children = new DbIterator[1];
        children[0] = child;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
	// some code goes here
        child = children[0];
    }
    
}

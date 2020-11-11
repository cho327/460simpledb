package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;
    private HashMap<String, Integer> map;
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
        this.gbfield = gbfield;
        this.afield = afield;
        this.gbfieldtype = gbfieldtype;
        this.what = what;
        if (what != Op.COUNT)
            throw new IllegalArgumentException("Invalid operator type " + what);
        map = new HashMap<String, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        String groupByFieldValue;
        Integer count;

        groupByFieldValue = (gbfield == NO_GROUPING ? "" : tup.getField(gbfield).toString());

        if (map.get(groupByFieldValue) == null)
        {
            count = 1;
            map.put(groupByFieldValue, count);
        }
        else
        {
            count = map.get(groupByFieldValue) +1 ;
            map.replace(groupByFieldValue, count);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        //String groupVal;
        Integer aggregateVal;
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();

        // set tuple desc
        // no grouping: one single aggregateVal
        // with grouping:  one pair (groupVal, aggregateVal)
        TupleDesc td;
        td = (gbfield == NO_GROUPING ? new TupleDesc(new Type[]{Type.INT_TYPE}) : new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE}));

        // convert hash map entries to iterator of Tuples
        // map entry is <groupVal, aggregateVal> pair
        for (String groupVal : map.keySet()) {
            aggregateVal = map.get(groupVal);
            Tuple tup = new Tuple(td);
            if (gbfield == NO_GROUPING)
                tup.setField(0, new IntField(aggregateVal));
            else {    // tuple has a pair
                if (gbfieldtype == Type.INT_TYPE)
                    tup.setField(0, new IntField(new Integer(groupVal)));
                else tup.setField(0, new StringField(groupVal, Type.STRING_LEN));
                tup.setField(1, new IntField(aggregateVal));
            }
            tuples.add(tup);
        }

        return new TupleIterator(td, tuples);
    }
}

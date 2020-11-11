package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbfield, afield;
    private Type gbfieldtype;
    private Op what;
    private HashMap<String, Integer> map;
    private HashMap<String, Integer> mapCount;
    private Integer totalCount = 0;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        map = new HashMap<String, Integer>();
        mapCount = new HashMap<String, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        String groupByFieldValue;
        Integer aggregateVal;
        Integer groupCountVal=0;

        groupByFieldValue = (gbfield == NO_GROUPING ? "" : tup.getField(gbfield).toString());

        Integer aValue = ((IntField) tup.getField(afield)).getValue();

        if (map.get(groupByFieldValue) == null)
        {
            map.put(groupByFieldValue, aValue);
            mapCount.put(groupByFieldValue, 1);
            totalCount ++;
            return;
        }

        aggregateVal = map.get(groupByFieldValue);
        groupCountVal = mapCount.get(groupByFieldValue);

        switch (what) {
            case MIN:
                aggregateVal = (aValue < aggregateVal ? aValue : aggregateVal);
                break;
            case MAX:
                aggregateVal = (aValue > aggregateVal ? aValue : aggregateVal);
                break;
            case SUM:
            case AVG:
//    		aggregateVal = ((aggregateVal * groupCountVal) + aValue) / (groupCountVal+1);
//    		System.out.println("GroupByFieldValue " + groupByFieldValue);
//    		System.out.println("aggreateVal " + aggregateVal);
//    		System.out.println("GroupCountVal " + groupCountVal);
//    		System.out.println("Aggregate current value " + aValue);
//    		aggregateVal += aValue;
//    		mapCount.replace(groupByFieldValue, groupCountVal + 1);
//    		break;
            case COUNT:
            case SUM_COUNT:
            case SC_AVG:
            default:
                aggregateVal += aValue;
                break;
        }
        map.replace(groupByFieldValue, aggregateVal);
        mapCount.replace(groupByFieldValue, groupCountVal + 1);
        totalCount++;
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        Integer aggregateVal, groupCount;
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();

        // set tuple desc
        // no grouping: one single aggregateVal
        // with grouping:  one pair (groupVal, aggregateVal)
        TupleDesc td;
        td = (gbfield == NO_GROUPING ? new TupleDesc(new Type[] {Type.INT_TYPE}) : new TupleDesc(new Type[] {gbfieldtype, Type.INT_TYPE}));

        // convert hash map entries to iterator of Tuples
        // map entry is <groupVal, aggregateVal> pair
        for (String groupVal : map.keySet())
        {
            aggregateVal = map.get(groupVal);
            groupCount = mapCount.get(groupVal);
            switch (what) {
                case COUNT:
                    aggregateVal = groupCount;
                    break;
                case AVG:
                    aggregateVal = aggregateVal/groupCount;
                    break;
                case MIN:
                case MAX:
                case SUM:
                case SUM_COUNT:
                case SC_AVG:
                default:
            }
            Tuple tup = new Tuple(td);
            if (gbfield == NO_GROUPING)
                tup.setField(0, new IntField(aggregateVal));
            else
            {	// tuple has a pair
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

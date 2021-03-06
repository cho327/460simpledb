package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class HashEquiJoin extends Operator {

    private static final long serialVersionUID = 1L;
    private JoinPredicate predicate;
    private DbIterator child1;
    private DbIterator child2;
    private TupleDesc td1, td2;
    private TupleDesc comboTD;
    transient private Tuple t1 = null;
    transient private Tuple t2 = null;


    HashMap<Object, ArrayList<Tuple>> map = new HashMap<Object, ArrayList<Tuple>>();
    public final static int MAP_SIZE = 20000;
    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    public HashEquiJoin(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        this.predicate = p;
        this.child1 = child1;
        this.child2 = child2;
        td1 = child1.getTupleDesc();
        td2 = child2.getTupleDesc();
        predicate = p;
        comboTD = TupleDesc.merge(td1, td2);
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        return predicate;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return comboTD;
    }
    
    public String getJoinField1Name()
    {
        // some code goes here
	return ("table1." + td1.getFieldName(predicate.getField1()));
    }

    public String getJoinField2Name()
    {
        // some code goes here
        return ("table2." + td2.getFieldName(predicate.getField2()));
    }
    
    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        child1.open();
        child2.open();
        super.open();
        loadMap();
    }

    public void close() {
        // some code goes here
        super.close();
        child1.close();
        child2.close();
        this.t1=null;
        this.t2=null;
        this.listIt=null;
        this.map.clear();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child1.rewind();
        child2.rewind();
    }

    transient Iterator<Tuple> listIt = null;

    // loadMap load tuples into hash map from Table 1 using key value from predicate.getField1 value
    // it loads up to MAP_SIZE into hash map from child1 iterator
    // hash map contains <key, ArrayList>
    private boolean loadMap() throws DbException, TransactionAbortedException {
        int cnt = 0;
        map.clear();

        // loop thru child1 iterator to collect rows into hash map
        // exit when partition is MAX_SIZE or no more rows from child1
        while (child1.hasNext()) {
            t1 = child1.next();
            ArrayList<Tuple> list = null;
//            System.out.println("Count: " + cnt);
//            System.out.println("tuple " + t1.toString());
//            System.out.println("field value" + t1.getField(predicate.getField1()));
            if (! map.containsKey(t1.getField(predicate.getField1()))) {
                list = new ArrayList<Tuple>();
                map.put(t1.getField(predicate.getField1()), list);
            }
            list = map.get(t1.getField(predicate.getField1()));
            list.add(t1);
            if (cnt++ == MAP_SIZE)
                return true;
        }
        return cnt > 0;

    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, there will be two copies of the join attribute in
     * the results. (Removing such duplicate columns can be done with an
     * additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */

    private Tuple processList() throws TransactionAbortedException, DbException {
        t1 = listIt.next();
        int td1n = t1.getTupleDesc().numFields();
        int td2n = t2.getTupleDesc().numFields();

        // set fields in combined tuple
        Tuple t = new Tuple(comboTD);
        for (int i = 0; i < td1n; i++)
            t.setField(i, t1.getField(i));
        for (int i = 0; i < td2n; i++)
            t.setField(td1n + i, t2.getField(i));
        return t;

    }

    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        // listIt contains rows from Table 1 matching join condition from Table 2's current row
        if (listIt != null && listIt.hasNext()) {
            return processList();
        }

        // loop around child2 to find matching rows matching the key in hash map
        while (child2.hasNext()) {
            t2 = child2.next();

            // if match, create a combined tuple and fill it with the values
            // from both tuples calling processList
            ArrayList<Tuple> matchList = map.get(t2.getField(predicate.getField2()));
            if (matchList == null)
                continue;
            listIt = matchList.iterator();
            return processList();
        }

        // child2 is done: advance child1 ie. load another MAX_SIZE hash map
        child2.rewind();
        if (loadMap()) {
            return fetchNext();
        }

        return null;
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        DbIterator[] children = new DbIterator[2];
        children[0] = child1;
        children[1] = child2;
        return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        child1 = children[0];
        child2 = children[1];
    }
    
}

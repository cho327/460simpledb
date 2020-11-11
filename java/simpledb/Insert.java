package simpledb;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tran;
    private DbIterator child;
    private int tableId;
    private Integer insertCount=0;
    private TupleIterator tupIter;

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
    public Insert(TransactionId t,DbIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tran = t;
        this.child = child;
        this.tableId = tableId;
        DbFile f = Database.getCatalog().getDatabaseFile(tableId);
        if (! child.getTupleDesc().equals(f.getTupleDesc()))
            throw new DbException("child TupleDesc not match table TupleDesc");
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return new TupleDesc(new Type[] {Type.INT_TYPE});
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        Tuple tup;
        child.open();
        super.open();
        while (child.hasNext()) {
            tup = child.next();
            try {
                Database.getBufferPool().insertTuple(tran, tableId, tup);
            } catch (DbException | IOException | TransactionAbortedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            insertCount ++;
        }
        TupleDesc td = new TupleDesc(new Type[] {Type.INT_TYPE});
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        Tuple t = new Tuple(td);
        t.setField(0, new IntField(new Integer(insertCount)));
        tuples.add(t);

        tupIter = new TupleIterator (td, tuples);
        tupIter.open();
    }

    public void close() {
        // some code goes here
        tupIter.close();
        child.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        child.rewind();
        tupIter.close();
        tupIter.open();
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
        if (tupIter.hasNext())
            return tupIter.next();
        else return null;
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

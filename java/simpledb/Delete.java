package simpledb;

import java.io.IOException;
import java.util.ArrayList;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;
    private TransactionId tran;
    private DbIterator child;
    //private int tableId;
    private Integer deleteCount=0;
    private TupleIterator tupIter;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        // some code goes here
        this.tran = t;
        this.child = child;
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
                Database.getBufferPool().deleteTuple(tran, tup);
            } catch (DbException | IOException | TransactionAbortedException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            deleteCount ++;
        }
        TupleDesc td = new TupleDesc(new Type[] {Type.INT_TYPE});
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        Tuple t = new Tuple(td);
        t.setField(0, new IntField(new Integer(deleteCount)));
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
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

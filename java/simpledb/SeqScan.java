package simpledb;

import javax.xml.crypto.Data;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements DbIterator {

    private static final long serialVersionUID = 1L;
    private TransactionId TranxID;
    private int TableID;
    private DbFile dbFile;
    private TupleDesc tupleDesc;
    private DbFileIterator iter;
    private String tableAlias;
    private int scanOpen=0;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.TranxID = tid;
        this.TableID = tableid;
        this.tableAlias = tableAlias;
        dbFile = Database.getCatalog().getDatabaseFile(tableid);
        tupleDesc = Database.getCatalog().getTupleDesc(tableid);
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(TableID);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.TableID = tableid;
        this.tableAlias = tableAlias;
        dbFile = Database.getCatalog().getDatabaseFile(tableid);
        tupleDesc = Database.getCatalog().getTupleDesc(tableid);
        iter = dbFile.iterator(TranxID);
    }

    public SeqScan(TransactionId tid, int tableid) {
        this(tid, tableid, Database.getCatalog().getTableName(tableid));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        try {
            iter = dbFile.iterator(TranxID);
            scanOpen = 1;
            iter.open();
        }
        catch (TransactionAbortedException e)
        {
            e.printStackTrace();
            throw new TransactionAbortedException();
        }
        catch (DbException e) {
            e.printStackTrace();
            throw new DbException("Database exception in opening dbFile");
        }
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        TupleDesc td = tupleDesc;

        int numFields = td.numFields();
        Type[] typeAr = new Type[numFields];
        String[] fieldAr = new String[numFields];

        for (int i = 0; i < numFields; i ++) {
            typeAr[i] = td.getFieldType(i);
            fieldAr[i] = tableAlias + "." + td.getFieldName(i);
        }

        return new TupleDesc(typeAr, fieldAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        try {
            if (scanOpen == 0) {
                throw new java.lang.IllegalStateException();
            }
            return iter != null && iter.hasNext();
        }
    	catch (TransactionAbortedException e)
        {
            e.printStackTrace();
            throw new TransactionAbortedException() ;
        }
    	catch (DbException e)
        {
            e.printStackTrace();
            throw new DbException("DB Exception in DbIterator has next") ;
        }
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if (scanOpen == 0) {
            throw new java.lang.IllegalStateException();
        }
        // if (!hasNext()) throw new NoSuchElementException("File Iterator is null.");
        try {
            if (hasNext()) {
                Tuple n = iter.next();
                // System.out.println("Tableid = " + tableId + " Seq Tuple = " + ((IntField)(n.getField(0))).getValue());
                return n;
            }
            throw new NoSuchElementException("SeqScan: No more tuple.");
        }
        catch (TransactionAbortedException e)
        {
            e.printStackTrace();
            throw new TransactionAbortedException();
        }
        catch (DbException e)
        {
            e.printStackTrace();
            throw new DbException("DB Exception in Tuple next") ;
        }
    }

    public void close() {
        // some code goes here
        dbFile = null;
        tupleDesc = null;
        scanOpen = 0;
        iter.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        if (scanOpen == 0) {
            throw new java.lang.IllegalStateException();
        }
        try {
            close();
            reset(TableID, tableAlias);
            open();
        }
        catch (TransactionAbortedException e)
        {
            e.printStackTrace();
            throw new TransactionAbortedException();
        }
    }
}

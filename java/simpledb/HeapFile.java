package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private TupleDesc TDesc;
    private File file;
    private int tableID;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.file = f;
        this.TDesc = td;
        this.tableID = getId();
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return TDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        HeapPageId id = (HeapPageId) pid;
        int pageSize = BufferPool.getPageSize();
        byte[] byteStream = new byte[pageSize];

        if (this.getId() != pid.getTableId())
            throw new java.lang.IllegalArgumentException();
        if (pid.pageNumber() < 0 || pid.pageNumber() >= this.numPages())
            throw new java.lang.IllegalArgumentException();
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(pageSize * pid.pageNumber());
            raf.readFully(byteStream);
            raf.close();
            return new HeapPage(id, byteStream);
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(file.length()/BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(this,tid);
    }

    private class HeapFileIterator implements DbFileIterator {

        private TransactionId tid;
        private HeapFile heapfile;

        private Page page;
        private HeapPageId pid;
        private Iterator<Tuple> pageIter;
        private int curPageNo = -1;

        public HeapFileIterator(HeapFile hf, TransactionId tid) {
            this.tid = tid;
            this.heapfile = hf;
        }


        public void open() throws
                DbException, TransactionAbortedException {
            try {
                if (tid.toString() == "simpledb.TransactionId@33")
                    System.out.println("Open call: curPage = " + curPageNo + " numPages = " + numPages());
                curPageNo = 0;
                pid = new HeapPageId(heapfile.tableID, curPageNo);
                page = (HeapPage) Database.getBufferPool().getPage(tid, pid,
                        Permissions.READ_ONLY);
                pageIter = ((HeapPage) page).iterator();
                return;
            } catch (TransactionAbortedException e) {
                e.printStackTrace();
                throw new TransactionAbortedException();
            } catch (DbException e) {
                e.printStackTrace();
                throw new DbException("DbException in HeapFileIterater open");
            }
        }

        public Tuple next() throws TransactionAbortedException,
                DbException, NoSuchElementException
        {
            if (pageIter == null) {
                throw new NoSuchElementException();
            }
            Tuple t = (hasNext()) ? (Tuple) pageIter.next(): null;
            if (!pageIter.hasNext())  {
                // advance the page
                while (curPageNo + 1 < numPages()) {
                    curPageNo ++;
//        			System.out.println("Next Tuple call: curPage = " + curPageNo + " numPages = " + numPages());
                    pid = new HeapPageId(heapfile.tableID, curPageNo);
                    page = (HeapPage) Database.getBufferPool().getPage(tid, pid,
                            Permissions.READ_ONLY);
                    pageIter = ((HeapPage) page).iterator();
                    if (!hasNext()) continue;
                    break;
                }
            }
            return t;
        }

        public boolean hasNext() throws DbException, TransactionAbortedException {
            return (pageIter != null) && (pageIter.hasNext());
        }

        public void rewind() throws DbException,
                TransactionAbortedException {
            try {
                close();
                open();
            }
            catch (TransactionAbortedException e)
            {
                e.printStackTrace();
                throw new TransactionAbortedException();
            }

        }

        public void close() {
            page = null;
            pid = null;
            pageIter = null;
            curPageNo = -1;
        }
    }

}


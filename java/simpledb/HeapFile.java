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

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
	File f;
	TupleDesc td;
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
    	this.f = f;
    	this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
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
    	return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
    	return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid)  {
        // some code goes here
    	HeapPageId hpid = (HeapPageId) pid;
    	byte[] pageByteData = HeapPage.createEmptyPageData();
    	int location = pid.pageNumber() * BufferPool.getPageSize();
    	RandomAccessFile accessFile;
		try {
			accessFile = new RandomAccessFile(f,"r");
			accessFile.seek(location);
			accessFile.read(pageByteData,0,BufferPool.getPageSize());
			accessFile.close();
			return new HeapPage(hpid,pageByteData);
		} catch (FileNotFoundException e1) {
			e1.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;
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
    	int ret = (int) f.length()/BufferPool.getPageSize();
        return ret;
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
    	return new HeapFileIterator(tid,this);
        
    }
    
    public class HeapFileIterator implements DbFileIterator {
    	
    	Iterator<Tuple> tupleIterator;
    	TransactionId tid;
    	HeapFile f;
    	int pageCursor;
    	
    	public HeapFileIterator(TransactionId tid, HeapFile f) {
    		this.tid = tid;
    		this.f = f;
    	}
    	
    	public void open() throws DbException, TransactionAbortedException {
    		pageCursor = 0;
    		tupleIterator = getPageTuples(0).iterator();
    	}
    	
    	public boolean hasNext() throws DbException, TransactionAbortedException {
    		if (tupleIterator == null) {
    			return false;
    		}
    		if (tupleIterator.hasNext() || (pageCursor+1 < f.numPages() && getPageTuples(pageCursor+1).size() > 0)) {
    			return true;
    		}
    		return false;
    	}
    	
    	public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
    		if (tupleIterator == null) {
    			throw new NoSuchElementException();
    		}
    		if (tupleIterator.hasNext()) {
    			return tupleIterator.next();
    		}
    		else if (!tupleIterator.hasNext() && pageCursor+1 < f.numPages()) {
    			pageCursor++;
    			tupleIterator = getPageTuples(pageCursor).iterator();
    		}
    		return tupleIterator.next();

    	}
    	
    	public ArrayList<Tuple> getPageTuples(int pgNo) throws DbException, TransactionAbortedException {
    		ArrayList<Tuple> ret = new ArrayList<Tuple>();
    		PageId pid = new HeapPageId(f.getId(),pgNo);
    		HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
    		
    		Iterator<Tuple> heapPageIt = page.iterator();
    		while (heapPageIt.hasNext()) {
    			ret.add(heapPageIt.next());
    		}
    		return ret;
    	}
    	
    	public void rewind() throws DbException, TransactionAbortedException {
    		close();
    		open();
    	}
    	
    	public void close() {
    		pageCursor = 0;
    		tupleIterator = null;
    	}
    }

}


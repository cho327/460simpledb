package simpledb;

import java.io.*;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.concurrent.ConcurrentHashMap;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 * 
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /** Bytes per page, including header. */
    private static final int PAGE_SIZE = 4096;

    private static int pageSize = PAGE_SIZE;
    
    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    private int numPages=16;
    private ConcurrentHashMap<PageId, Page> pageMap;
    private LRUCache lruCache;
    
    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
    	this.numPages = numPages;
        pageMap = new ConcurrentHashMap<PageId, Page>();
        this.lruCache = new LRUCache(DEFAULT_PAGES);
    }
    
    public static int getPageSize() {
      return pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
    	BufferPool.pageSize = pageSize;
    }
    
    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
    	BufferPool.pageSize = PAGE_SIZE;
    }
    
    private class LRUCache { 

    	Set<PageId> cache; 
    	int capacity; 

    	public LRUCache(int capacity) 
    	{ 
    		this.cache = new LinkedHashSet<PageId>(capacity); 
    		this.capacity = capacity; 
    	} 

    	// This function returns false if key is not 
    	// present in cache. Else it moves the key to 
    	// front by first removing it and then adding 
    	// it, and returns true. 
    	public boolean get(PageId key) 
    	{ 
    		if (!cache.contains(key)) 
    			return false; 
    		cache.remove(key); 
    		cache.add(key); 
    		return true; 
    	} 

    	/* Refers key x with in the LRU cache */
    	public void refer(PageId key) 
    	{		 
    		if (get(key) == false) 
    		put(key); 
    	} 

    	// display contents of cache 
    	public void display() 
    	{ 
    		Iterator<PageId> itr = cache.iterator(); 
    		while (itr.hasNext()) { 
    			System.out.print(itr.next() + " "); 
    		} 
    	} 
    	
    	public void put(PageId key) 
    	{ 
    		// If already present, then 
    		// remove it first. Note that 
    		// we are going to add later 
    		if (cache.contains(key)) 
    			cache.remove(key); 

    		// If cache size is full, remove the least 
    		// recently used. 
    		else if (cache.size() == capacity) { 
    			PageId firstKey = lruFirstKey();
    			lruRemove(firstKey);
    		} 

    		cache.add(key); 
    	} 
    	
    	public PageId lruFirstKey()
    	{ 
    		PageId firstKey = cache.iterator().next(); 
    		//cache.remove(firstKey); 
    		return firstKey;
    		
    	}
    	
    	public void lruRemove(PageId key)
    	{
    		cache.remove(key);
    	}

    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public  Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // some code goes here
    	if (!pageMap.containsKey(pid)) {
            if (pageMap.size() == numPages)
                evictPage();
//            if (tid.toString() == "simpledb.TransactionId@33")
//            	System.out.println("BufferPool getPage:  Trx id : " + tid + "  Page ID =" + pid );
            pageMap.put(pid, Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid));
            lruCache.put(pid);
            pageMap.get(pid).setBeforeImage();
         }
    	if (perm==Permissions.READ_WRITE)
    		pageMap.get(pid).markDirty(true, tid);
    	return pageMap.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return false;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other 
     * pages that are updated (Lock acquisition is not needed for lab2). 
     * May block if the lock(s) cannot be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
    	DbFile file = Database.getCatalog().getDatabaseFile(tableId);
    	ArrayList<Page> pageList = file.insertTuple(tid, t);
    	
    	// Now let's insert all dirty pages back to BufferPool
        for (Page p : pageList) {
            PageId pid = p.getId();
            p.markDirty(true, tid);
            if (pageMap.containsKey(pid)) {// in the buffer
            	pageMap.replace(pid,p);
            	lruCache.refer(pid);
           } 
           else {// not in buffer
        	   if ((!pageMap.containsKey(pid)) && (pageMap.size() == numPages)) evictPage();
        	   pageMap.put(pid, p);
        	   lruCache.put(pid);
           }
        }                 
//        System.out.println("Buffer pool succeeds to insert tuple: Tid is" + tid.toString() + " Insert Tuple is" + ((IntField)(t.getField(0))).getValue());
    	
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have 
     * been dirtied to the cache (replacing any existing versions of those pages) so 
     * that future requests see up-to-date pages. 
     *
     * @param tid the transaction deleting the tuple.
     * @param t the tuple to delete
     */
    public  void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1

        DbFile file = Database.getCatalog().getDatabaseFile(t.getRecordId().getPageId().getTableId() );
    	ArrayList<Page> pageList = file.deleteTuple(tid, t);
        
        for (Page p : pageList) {
            PageId pid = p.getId();
            p.markDirty(true, tid);
            if (pageMap.containsKey(pid)) {// in the cache
            	pageMap.replace(pid,p);
            	lruCache.refer(pid);
           	} 
           else {// not in cache
        	   if ((!pageMap.containsKey(pid)) && (pageMap.size() == numPages)) evictPage();
        	   pageMap.put(pid, p);
        	   lruCache.put(pid);
           }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
    	ConcurrentHashMap.KeySetView<PageId, Page> keySetView = pageMap.keySet();
        Iterator<PageId> iterator = keySetView.iterator();

        while (iterator.hasNext()) {
            PageId pid = iterator.next();
           //  output += key + "=>" + value + "; ";
            flushPage(pid);
        }
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
        
        Also used by B+ tree files to ensure that deleted pages
        are removed from the cache so they can be reused safely
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
    	pageMap.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes her
        // not necessary for lab1
    	Page page = pageMap.get(pid);
    	if (page != null && page.isDirty() != null) {
    		Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(page);
//    		System.out.println("bufferpool FlushPage: write to disk " + pid);
    		page.markDirty(false, null);
    		page.setBeforeImage();
    	}
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for lab1
    	Set<PageId> pids = pageMap.keySet();

    	// collect all clean pages in an array list
    	ArrayList<PageId> cleanPages = new ArrayList<PageId>();
    	PageId evictCandidate = null; 
    	
        for (PageId pid : pids) {
            if (pageMap.get(pid).isDirty() == null) {
                cleanPages.add(pid);
            }
        }
    	
        // if no clean page to evict, use the LRUCache to evict page
        if (cleanPages.size() == 0)  {
        	evictCandidate = lruCache.lruFirstKey();
        }
        else {
        // randomly select a candidate from cleanPages
        	evictCandidate = cleanPages.get((int) Math.floor(Math.random() * cleanPages.size()));
        	if (pageMap.get(evictCandidate).isDirty() != null)
        			throw new DbException("this page should be clean!");
        }
        try {
//    		System.out.println("bufferpool evictionPage " + evictCandidate);       	
        	flushPage(evictCandidate);        	
        	lruCache.lruRemove(evictCandidate); 
        	discardPage(evictCandidate);
        } 
        catch (IOException e) {
        		e.printStackTrace();
        }
    }
    		
}
    
 


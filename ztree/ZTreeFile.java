package ztree;

import java.io.*;

import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import java.util.*;

/** btfile.java
 * This is the main definition of class BTreeFile, which derives from
 * abstract base class IndexFile.
 * It provides an insert/delete interface.
 */
public class ZTreeFile extends ZIndexFile
        implements GlobalConst {

    private final static int MAGIC0 = 1989;

    private final static String lineSep = System.getProperty("line.separator");

    private static FileOutputStream fos;
    private static DataOutputStream trace;

    /**
     * It causes a structured trace to be written to a
     * file.  This output is
     * used to drive a visualization tool that shows the inner workings of the
     * b-tree during its operations.
     *
     * @param filename input parameter. The trace file name
     * @throws IOException error from the lower layer
     */
    public static void traceFilename(String filename)
            throws IOException {

        fos = new FileOutputStream(filename);
        trace = new DataOutputStream(fos);
    }

    /**
     * Stop tracing. And close trace file.
     *
     * @throws IOException error from the lower layer
     */
    public static void destroyTrace()
            throws IOException {
        if (trace != null) trace.close();
        if (fos != null) fos.close();
        fos = null;
        trace = null;
    }

    private ZTreeHeaderPage headerPage;
    private PageId headerPageId;
    private String dbname;

    /**
     * Access method to data member.
     *
     * @return Return a BTreeHeaderPage object that is the header page
     * of this btree file.
     */
    public ZTreeHeaderPage getHeaderPage() {
        return headerPage;
    }

    private PageId get_file_entry(String filename)
            throws GetFileEntryException {
        try {
            return SystemDefs.JavabaseDB.get_file_entry(filename);
        } catch (Exception e) {
            e.printStackTrace();
            throw new GetFileEntryException(e, "");
        }
    }

    private Page pinPage(PageId pageno)
            throws PinPageException {
        try {
            Page page = new Page();
            SystemDefs.JavabaseBM.pinPage(pageno, page, false/*Rdisk*/);
            return page;
        } catch (Exception e) {
            e.printStackTrace();
            throw new PinPageException(e, "");
        }
    }

    private void add_file_entry(String fileName, PageId pageno)
            throws AddFileEntryException {
        try {
            SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
        } catch (Exception e) {
            e.printStackTrace();
            throw new AddFileEntryException(e, "");
        }
    }

    private void unpinPage(PageId pageno)
            throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }

    private void freePage(PageId pageno)
            throws FreePageException {
        try {
            SystemDefs.JavabaseBM.freePage(pageno);
        } catch (Exception e) {
            e.printStackTrace();
            throw new FreePageException(e, "");
        }

    }

    private void delete_file_entry(String filename)
            throws DeleteFileEntryException {
        try {
            SystemDefs.JavabaseDB.delete_file_entry(filename);
        } catch (Exception e) {
            e.printStackTrace();
            throw new DeleteFileEntryException(e, "");
        }
    }

    private void unpinPage(PageId pageno, boolean dirty)
            throws UnpinPageException {
        try {
            SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
        } catch (Exception e) {
            e.printStackTrace();
            throw new UnpinPageException(e, "");
        }
    }

    /**
     * ZTreeFile class
     * an index file with given filename should already exist; this opens it.
     *
     * @param filename the B+ tree file name. Input parameter.
     * @throws GetFileEntryException  can not ger the file from DB
     * @throws PinPageException       failed when pin a page
     * @throws ConstructPageException BT page constructor failed
     */
    public ZTreeFile(String filename)
            throws GetFileEntryException,
            PinPageException,
            ConstructPageException {


        headerPageId = get_file_entry(filename);

        headerPage = new ZTreeHeaderPage(headerPageId);
        dbname = filename;
      /*
       *
       * - headerPageId is the PageId of this BTreeFile's header page;
       * - headerPage, headerPageId valid and pinned
       * - dbname contains a copy of the name of the database
       */
    }

    public ZTreeFile(String filename, int keytype,
                      int keysize, int delete_fashion)
            throws GetFileEntryException,
            ConstructPageException,
            IOException,
            AddFileEntryException {


        headerPageId = get_file_entry(filename);
        if (headerPageId == null) //file not exist
        {
            headerPage = new ZTreeHeaderPage();
            headerPageId = headerPage.getPageId();
            add_file_entry(filename, headerPageId);
            headerPage.set_magic0(MAGIC0);
            headerPage.set_rootId(new PageId(INVALID_PAGE));
            headerPage.set_keyType((short) keytype);
            headerPage.set_maxKeySize(keysize);
            headerPage.set_deleteFashion(delete_fashion);
            headerPage.setType(NodeType.ZTHEAD);
        } else {
            headerPage = new ZTreeHeaderPage(headerPageId);
        }

        dbname = new String(filename);

    }

    public void close()
            throws PageUnpinnedException,
            InvalidFrameNumberException,
            HashEntryNotFoundException,
            ReplacerException {
        if (headerPage != null) {
            SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
            headerPage = null;
        }
    }

    public void destroyFile()
            throws IOException,
            IteratorException,
            UnpinPageException,
            FreePageException,
            DeleteFileEntryException,
            ConstructPageException,
            PinPageException {
        if (headerPage != null) {
            PageId pgId = headerPage.get_rootId();
            if (pgId.pid != INVALID_PAGE)
                _destroyFile(pgId);
            unpinPage(headerPageId);
            freePage(headerPageId);
            delete_file_entry(dbname);
            headerPage = null;
        }
    }

    private void _destroyFile(PageId pageno)
            throws IOException,
            IteratorException,
            PinPageException,
            ConstructPageException,
            UnpinPageException,
            FreePageException {

        ZTSortedPage sortedPage;
        Page page = pinPage(pageno);
        sortedPage = new ZTSortedPage(page, headerPage.get_keyType());

        if (sortedPage.getType() == NodeType.INDEX) {
            ZTIndexPage indexPage = new ZTIndexPage(page, headerPage.get_keyType());
            NID nid = new NID();
            PageId childId;
            DescriptorKeyDataEntry entry;
            for (entry = indexPage.getFirst(nid);
                 entry != null; entry = indexPage.getNext(nid)) {
                childId = ((IndexData) (entry.data)).getData();
                _destroyFile(childId);
            }
        } else { // BTLeafPage

            unpinPage(pageno);
            freePage(pageno);
        }

    }

    private void updateHeader(PageId newRoot)
            throws IOException,
            PinPageException,
            UnpinPageException {

        ZTreeHeaderPage header;
        PageId old_data;


        header = new ZTreeHeaderPage(pinPage(headerPageId));

        old_data = headerPage.get_rootId();
        header.set_rootId(newRoot);

        // clock in dirty bit to bm so our dtor needn't have to worry about it
        unpinPage(headerPageId, true /* = DIRTY */);


        // ASSERTIONS:
        // - headerPage, headerPageId valid, pinned and marked as dirty

    }

    public void insert(KeyClass key, NID nid) throws KeyTooLongException, KeyNotMatchException,
            LeafInsertRecException, IndexInsertRecException, ConstructPageException,
            UnpinPageException, PinPageException, NodeNotMatchException,
            ConvertException, DeleteRecException, IndexSearchException, IteratorException,
            LeafDeleteException, InsertException,
            IOException 
            {
                DescriptorKeyDataEntry newRootEntry;

        if (ZT.getKeyLength(key) > headerPage.get_maxKeySize()) {
            throw new KeyTooLongException(null, "");
        }

        if (key instanceof DescriptorKey) {
            if (headerPage.get_keyType() != AttrType.attrDesc) {
                throw new KeyNotMatchException(null, "");
            }
        } else {
            throw new KeyNotMatchException(null, "");
        }

        if (trace != null) {
            trace.writeBytes("INSERT " + nid.pageNo + " "
                    + nid.slotNo + " " + key + lineSep);
            trace.writeBytes("DO" + lineSep);
            trace.flush();
        }

        if (headerPage.get_rootId().pid == INVALID_PAGE) {
            PageId newRootPageId;
            ZTLeafPage newRootPage;
            NID dummynid;

            newRootPage = new ZTLeafPage(headerPage.get_keyType());
            newRootPageId = newRootPage.getCurPage();

            if (trace != null) {
                trace.writeBytes("NEWROOT " + newRootPageId + lineSep);
                trace.flush();
            }

            newRootPage.setNextPage(new PageId(INVALID_PAGE));
            newRootPage.setPrevPage(new PageId(INVALID_PAGE));
            newRootPage.insertRecord(key, nid);

            if ( trace!=null ) {
                trace.writeBytes("PUTIN node " + newRootPageId+lineSep);
                trace.flush();
            }

            unpinPage(newRootPageId, true); /* = DIRTY */
            updateHeader(newRootPageId);

            if ( trace!=null ) {
                trace.writeBytes("DONE" + lineSep);
                trace.flush();
            }

            return;
        }

        if ( trace != null ) {
            trace.writeBytes( "SEARCH" + lineSep);
            trace.flush();
        }

        newRootEntry = _insert(key, nid, headerPage.get_rootId());

        if (newRootEntry != null) {
            ZTIndexPage newRootPage;
            PageId newRootPageId;
            Object newEntryKey;

            // the information about the pair <key, PageId> is
            // packed in newRootEntry: extract it

            newRootPage = new ZTIndexPage(headerPage.get_keyType());
            newRootPageId = newRootPage.getCurPage();

            // ASSERTIONS:
            // - newRootPage, newRootPageId valid and pinned
            // - newEntryKey, newEntryPage contain the data for the new entry
            //     which was given up from the level down in the recursion

            if ( trace != null ) {
                trace.writeBytes("NEWROOT " + newRootPageId + lineSep);
                trace.flush();
            }

            newRootPage.insertKey( newRootEntry.key,
                    ((IndexData)newRootEntry.data).getData() );

            // the old root split and is now the left child of the new root
            newRootPage.setPrevPage(headerPage.get_rootId());
            unpinPage(newRootPageId, true /* = DIRTY */);
            updateHeader(newRootPageId);
        }

        if ( trace !=null ) {
            trace.writeBytes("DONE"+lineSep);
            trace.flush();
        }

        return;
    }


    private DescriptorKeyDataEntry _insert(KeyClass key, NID nid, PageId currentPageId) throws PinPageException,
            IOException, ConstructPageException, LeafDeleteException, ConstructPageException, DeleteRecException,
            IndexSearchException, UnpinPageException, LeafInsertRecException, ConvertException, IteratorException,
            IndexInsertRecException, KeyNotMatchException, NodeNotMatchException, InsertException {
        ZTSortedPage currentPage;
        Page page;
        DescriptorKeyDataEntry upEntry;

        page = pinPage(currentPageId);
        currentPage = new ZTSortedPage(page, headerPage.get_keyType());

        if (trace != null) {
            trace.writeBytes("VISIT node " + currentPageId + lineSep);
            trace.flush();
        }

        // TWO CASES:
        // - pageType == INDEX:
        //   recurse and then split if necessary
        // - pageType == LEAF:
        //   try to insert pair (key, rid), maybe split

        if (currentPage.getType() == NodeType.INDEX) {
            ZTIndexPage currentIndexPage = new ZTIndexPage(page, headerPage.get_keyType());
            PageId currentIndexPageId = currentPageId;
            PageId nextPageId;

            nextPageId = currentIndexPage.getPageNoByKey(key);
            // now unpin the page, recurse and then pin it again
            unpinPage(currentIndexPageId);
            upEntry = _insert(key, nid, nextPageId);

            // two cases:
            // - upEntry == null: one level lower no split has occurred:
            //                     we are done.
            // - upEntry != null: one of the children has split and
            //                    upEntry is the new data entry which has
            //                    to be inserted on this index page

            if (upEntry == null) {
                return null;
            }

            currentIndexPage = new ZTIndexPage(pinPage(currentPageId), headerPage.get_keyType());

            // ASSERTIONS:
            // - upEntry != null
            // - currentIndexPage, currentIndexPageId valid and pinned

            // the information about the pair <key, PageId> is
            // packed in upEntry

            // check whether there can still be entries inserted on that page
            if (currentIndexPage.available_space() >=
                    ZT.getKeyDataLength(upEntry.key, NodeType.INDEX)) {
                // no split has occurred
                currentIndexPage.insertKey(upEntry.key, ((IndexData) upEntry.data).getData());
                unpinPage(currentIndexPageId, true /* DIRTY */);
                return null;
            }
            // ASSERTIONS:
            // - on the current index page is not enough space available .
            //   it splits

            //   therefore we have to allocate a new index page and we will
            //   distribute the entries
            // - currentIndexPage, currentIndexPageId valid and pinned

            ZTIndexPage newIndexPage;
            PageId newIndexPageId;

            // we have to allocate a new INDEX page and
            // to redistribute the index entries
            newIndexPage = new ZTIndexPage(headerPage.get_keyType());
            newIndexPageId = newIndexPage.getCurPage();

            if (trace != null) {
                if (headerPage.get_rootId().pid != currentIndexPageId.pid) {
                    trace.writeBytes("SPLIT node " + currentIndexPageId
                            + " IN nodes " + currentIndexPageId +
                            " " + newIndexPageId + lineSep);
                } else {
                    trace.writeBytes("ROOTSPLIT IN nodes " + currentIndexPageId
                            + " " + newIndexPageId + lineSep);
                }
                trace.flush();
            }

            // ASSERTIONS:
            // - newIndexPage, newIndexPageId valid and pinned
            // - currentIndexPage, currentIndexPageId valid and pinned
            // - upEntry containing (Key, Page) for the new entry which was
            //     given up from the level down in the recursion

            DescriptorKeyDataEntry tmpEntry;
            PageId tmpPageId;
            NID insertNid;
            NID delNid = new NID();

            for (tmpEntry = currentIndexPage.getFirst(delNid);
                 tmpEntry != null; tmpEntry = currentIndexPage.getFirst(delNid)) {
                newIndexPage.insertKey(tmpEntry.key,
                        ((IndexData) tmpEntry.data).getData());
                currentIndexPage.deleteSortedRecord(delNid);
            }

            // ASSERTIONS:
            // - currentIndexPage empty
            // - newIndexPage holds all former records from currentIndexPage

            // we will try to make an equal split
            NID firstNid = new NID();
            DescriptorKeyDataEntry undoEntry = null;
            for (tmpEntry = newIndexPage.getFirst(firstNid);
                 (currentIndexPage.available_space() > newIndexPage.available_space());
                 tmpEntry = newIndexPage.getFirst(firstNid)) {
                // now insert the <key,pageId> pair on the new
                // index page
                undoEntry = tmpEntry;
                currentIndexPage.insertKey(tmpEntry.key,
                        ((IndexData) tmpEntry.data).getData());
                newIndexPage.deleteSortedRecord(firstNid);
            }

            //undo the final record
            if (currentIndexPage.available_space() <= newIndexPage.available_space()) {
                newIndexPage.insertKey(undoEntry.key,
                        ((IndexData) undoEntry.data).getData());
                currentIndexPage.deleteSortedRecord
                        (new NID(currentIndexPage.getCurPage(), (int) currentIndexPage.getSlotCnt() - 1));
            }

            // check whether <newKey, newIndexPageId>
            // will be inserted
            // on the newly allocated or on the old index page

            tmpEntry = newIndexPage.getFirst(firstNid);

            if (ZT.keyCompare(upEntry.key, tmpEntry.key) >= 0) {
                // the new data entry belongs on the new index page
                newIndexPage.insertKey(upEntry.key,
                        ((IndexData) upEntry.data).getData());
            } else {
                currentIndexPage.insertKey(upEntry.key,
                        ((IndexData) upEntry.data).getData());

                int i = (int) currentIndexPage.getSlotCnt() - 1;
                tmpEntry = ZT.getEntryFromBytes(currentIndexPage.getpage(),
                        currentIndexPage.getSlotOffset(i),
                        currentIndexPage.getSlotLength(i),
                        headerPage.get_keyType(), NodeType.INDEX);

                newIndexPage.insertKey(tmpEntry.key,
                        ((IndexData) tmpEntry.data).getData());

                currentIndexPage.deleteSortedRecord
                        (new NID(currentIndexPage.getCurPage(), i));
            }
            unpinPage(currentIndexPageId, true /* dirty */);

            // fill upEntry
            upEntry = newIndexPage.getFirst(delNid);

            // now set prevPageId of the newIndexPage to the pageId
            // of the deleted entry:
            newIndexPage.setPrevPage(((IndexData) upEntry.data).getData());

            // delete first record on new index page since it is given up
            newIndexPage.deleteSortedRecord(delNid);

            unpinPage(newIndexPageId, true /* dirty */);

            if (trace != null) {
                trace_children(currentIndexPageId);
                trace_children(newIndexPageId);
            }

            ((IndexData) upEntry.data).setData(newIndexPageId);

            return upEntry;
        } else if (currentPage.getType() == NodeType.LEAF) {
            ZTLeafPage currentLeafPage =
                    new ZTLeafPage(page, headerPage.get_keyType());

            PageId currentLeafPageId = currentPageId;

            // ASSERTIONS:
            // - currentLeafPage, currentLeafPageId valid and pinned

            // check whether there can still be entries inserted on that page
            if (currentLeafPage.available_space() >= ZT.getKeyDataLength(key, NodeType.LEAF)) {
                // no split has occurred
                currentLeafPage.insertRecord(key, nid);
                unpinPage(currentLeafPageId, true /* DIRTY */);

                if (trace != null) {
                    trace.writeBytes("PUTIN node " + currentLeafPageId + lineSep);
                    trace.flush();
                }

                return null;
            }

            // ASSERTIONS:
            // - on the current leaf page is not enough space available.
            //   It splits.
            // - therefore we have to allocate a new leaf page and we will
            // - distribute the entries

            ZTLeafPage newLeafPage;
            PageId newLeafPageId;
            // we have to allocate a new LEAF page and
            // to redistribute the data entries entries
            newLeafPage = new ZTLeafPage(headerPage.get_keyType());
            newLeafPageId = newLeafPage.getCurPage();

            newLeafPage.setNextPage(currentLeafPage.getNextPage());
            newLeafPage.setPrevPage(currentLeafPageId);  // for dbl-linked list
            currentLeafPage.setNextPage(newLeafPageId);
            // change the prevPage pointer on the next page:

            PageId rightPageId;
            rightPageId = newLeafPage.getNextPage();
            if (rightPageId.pid != INVALID_PAGE) {
                ZTLeafPage rightPage;
                rightPage = new ZTLeafPage(rightPageId, headerPage.get_keyType());

                rightPage.setPrevPage(newLeafPageId);
                unpinPage(rightPageId, true /* = DIRTY */);

                // ASSERTIONS:
                // - newLeafPage, newLeafPageId valid and pinned
                // - currentLeafPage, currentLeafPageId valid and pinned
            }

            if (trace != null) {
                if (headerPage.get_rootId().pid != currentLeafPageId.pid) {
                    trace.writeBytes("SPLIT node " + currentLeafPageId
                            + " IN nodes "
                            + currentLeafPageId + " " + newLeafPageId + lineSep);
                } else {
                    trace.writeBytes("ROOTSPLIT IN nodes " + currentLeafPageId
                            + " " + newLeafPageId + lineSep);
                }
                trace.flush();
            }

            DescriptorKeyDataEntry tmpEntry;
            NID firstNid = new NID();


            for (tmpEntry = currentLeafPage.getFirst(firstNid);
                 tmpEntry != null; tmpEntry = currentLeafPage.getFirst(firstNid)) {
                newLeafPage.insertRecord(tmpEntry.key,
                        ((LeafData) (tmpEntry.data)).getData());
                currentLeafPage.deleteSortedRecord(firstNid);
            }

            // ASSERTIONS:
            // - currentLeafPage empty
            // - newLeafPage holds all former records from currentLeafPage

            DescriptorKeyDataEntry undoEntry = null;
            for (tmpEntry = newLeafPage.getFirst(firstNid);
                 newLeafPage.available_space() < currentLeafPage.available_space();
                 tmpEntry = newLeafPage.getFirst(firstNid)) {
                undoEntry = tmpEntry;
                currentLeafPage.insertRecord(tmpEntry.key,
                        ((LeafData) tmpEntry.data).getData());
                newLeafPage.deleteSortedRecord(firstNid);
            }

            if (ZT.keyCompare(key, undoEntry.key) < 0) {
                //undo the final record
                if (currentLeafPage.available_space() <
                        newLeafPage.available_space()) {
                    newLeafPage.insertRecord(undoEntry.key,
                            ((LeafData) undoEntry.data).getData());

                    currentLeafPage.deleteSortedRecord
                            (new NID(currentLeafPage.getCurPage(),
                                    (int) currentLeafPage.getSlotCnt() - 1));
                }
            }

            // check whether <key, rid>
            // will be inserted
            // on the newly allocated or on the old leaf page

            if (ZT.keyCompare(key, undoEntry.key) >= 0) {
                // the new data entry belongs on the new Leaf page
                newLeafPage.insertRecord(key, nid);
                if (trace != null) {
                    trace.writeBytes("PUTIN node " + newLeafPageId + lineSep);
                    trace.flush();
                }
            } else {
                currentLeafPage.insertRecord(key, nid);
            }

            unpinPage(currentLeafPageId, true /* dirty */);

            if (trace != null) {
                trace_children(currentLeafPageId);
                trace_children(newLeafPageId);
            }
            // fill upEntry
            tmpEntry = newLeafPage.getFirst(firstNid);
            upEntry = new DescriptorKeyDataEntry(tmpEntry.key, newLeafPageId);

            unpinPage(newLeafPageId, true /* dirty */);

            // ASSERTIONS:
            // - no pages pinned
            // - upEntry holds the valid KeyDataEntry which is to be inserted
            // on the index page one level up
            return upEntry;
        } else {
            throw new InsertException(null, "");
        }
    }

    public boolean Delete(KeyClass key, NID nid) throws DeleteFashionException, LeafRedistributeException,
            RedistributeException, InsertRecException, KeyNotMatchException, UnpinPageException,
            IndexInsertRecException, FreePageException, RecordNotFoundException, PinPageException,
            IndexFullDeleteException, LeafDeleteException, IteratorException, ConstructPageException,
            DeleteRecException, IndexSearchException, IOException 
            {
        if (headerPage.get_deleteFashion() == DeleteFashion.FULL_DELETE) {
            return FullDelete(key, nid);
        } else if (headerPage.get_deleteFashion() == DeleteFashion.NAIVE_DELETE) {
            return NaiveDelete(key, nid);
        } else {
            throw new DeleteFashionException(null, "");
        }
    }

    public ZTLeafPage findRunStart (KeyClass lo_key, NID startnid) throws IOException, IteratorException,
            KeyNotMatchException, ConstructPageException, PinPageException, UnpinPageException {
        ZTLeafPage pageLeaf;
        ZTIndexPage pageIndex;
        Page page;
        ZTSortedPage sortPage;
        PageId pageno;
        PageId curpageno=null;                // iterator
        PageId prevpageno;
        PageId nextpageno;
        RID curRid;
        DescriptorKeyDataEntry curEntry;

        pageno = headerPage.get_rootId();

        if (pageno.pid == INVALID_PAGE) {        // no pages in the BTREE
            pageLeaf = null;                // should be handled by
            // startrid =INVALID_PAGEID ;             // the caller
            return pageLeaf;
        }

        page= pinPage(pageno);
        sortPage = new ZTSortedPage(page, headerPage.get_keyType());

        if ( trace!=null ) {
            trace.writeBytes("VISIT node " + pageno + lineSep);
            trace.flush();
        }

        // ASSERTION
        // - pageno and sortPage is the root of the btree
        // - pageno and sortPage valid and pinned

        while (sortPage.getType() == NodeType.INDEX) {
            pageIndex = new ZTIndexPage(page, headerPage.get_keyType());
            prevpageno = pageIndex.getPrevPage();
            curEntry= pageIndex.getFirst(startnid);
            while ( curEntry!=null && lo_key != null && ZT.keyCompare(curEntry.key, lo_key) < 0) {
                prevpageno = ((IndexData)curEntry.data).getData();
                curEntry=pageIndex.getNext(startnid);
            }

            unpinPage(pageno);
            pageno = prevpageno;
            page=pinPage(pageno);
            sortPage=new ZTSortedPage(page, headerPage.get_keyType());

            if ( trace!=null ) {
                trace.writeBytes( "VISIT node " + pageno+lineSep);
                trace.flush();
            }
        }

        pageLeaf = new ZTLeafPage(page, headerPage.get_keyType() );

        curEntry=pageLeaf.getFirst(startnid);
        while (curEntry==null) {
            // skip empty leaf pages off to left
            nextpageno = pageLeaf.getNextPage();
            unpinPage(pageno);
            if (nextpageno.pid == INVALID_PAGE) {
                // oops, no more records, so set this scan to indicate this.
                return null;
            }

            pageno = nextpageno;
            pageLeaf=  new ZTLeafPage( pinPage(pageno), headerPage.get_keyType());
            curEntry=pageLeaf.getFirst(startnid);
        }

        // ASSERTIONS:
        // - curkey, curRid: contain the first record on the
        //     current leaf page (curkey its key, cur
        // - pageLeaf, pageno valid and pinned

        if (lo_key == null) {
            return pageLeaf;
            // note that pageno/pageLeaf is still pinned;
            // scan will unpin it when done
        }

        while (ZT.keyCompare(curEntry.key, lo_key) < 0) {
            curEntry= pageLeaf.getNext(startnid);
            while (curEntry == null) { // have to go right
                nextpageno = pageLeaf.getNextPage();
                unpinPage(pageno);

                if (nextpageno.pid == INVALID_PAGE) {
                    return null;
                }

                pageno = nextpageno;
                pageLeaf=new ZTLeafPage(pinPage(pageno), headerPage.get_keyType());
                curEntry=pageLeaf.getFirst(startnid);
            }
        }

        return pageLeaf;
    }

    private boolean NaiveDelete( KeyClass key, NID nid) throws LeafDeleteException,
            KeyNotMatchException, PinPageException, IOException,
            UnpinPageException, PinPageException, IndexSearchException, IteratorException, ConstructPageException {
        ZTLeafPage leafPage;
        NID curNid=new NID();  // iterator
        KeyClass curkey;
        NID dummynid;
        PageId nextpage;
        boolean deleted;
        DescriptorKeyDataEntry entry;

        if ( trace!=null ) {
            trace.writeBytes("DELETE " +nid.pageNo +" " + nid.slotNo + " "
                    + key +lineSep);
            trace.writeBytes( "DO"+lineSep);
            trace.writeBytes( "SEARCH" +lineSep);
            trace.flush();
        }

        leafPage = findRunStart(key, curNid);  // find first page,rid of key
        if( leafPage == null) {
            return false;
        }
        entry=leafPage.getCurrent(curNid);

        while (true) {
            while (entry == null) { // have to go right
                nextpage = leafPage.getNextPage();
                unpinPage(leafPage.getCurPage());
                if (nextpage.pid == INVALID_PAGE) {
                    return false;
                }

                leafPage = new ZTLeafPage(pinPage(nextpage),
                        headerPage.get_keyType() );
                entry=leafPage.getFirst(new NID());
            }

            if (ZT.keyCompare(key, entry.key) > 0 )
                break;

            if (leafPage.delEntry(new DescriptorKeyDataEntry(key, nid))) {
                // successfully found <key, rid> on this page and deleted it.
                // unpin dirty page and return OK.
                unpinPage(leafPage.getCurPage(), true /* = DIRTY */);

                if ( trace!=null ) {
                    trace.writeBytes( "TAKEFROM node " + leafPage.getCurPage()+lineSep);
                    trace.writeBytes("DONE"+lineSep);
                    trace.flush();
                }

                return true;
            }

            nextpage = leafPage.getNextPage();
            unpinPage(leafPage.getCurPage());

            leafPage = new ZTLeafPage(pinPage(nextpage), headerPage.get_keyType());

            entry=leafPage.getFirst(curNid);
        }
      /*
       * We reached a page with first key > `key', so return an error.
       * We should have got true back from delUserRid above.  Apparently
       * the specified <key,rid> data entry does not exist.
       */
        unpinPage(leafPage.getCurPage());
        return false;
    }

    private boolean FullDelete (KeyClass key,  NID nid) throws IndexInsertRecException,
            RedistributeException, IndexSearchException, RecordNotFoundException,
            DeleteRecException, InsertRecException, LeafRedistributeException,
            IndexFullDeleteException, FreePageException, LeafDeleteException, KeyNotMatchException,
            ConstructPageException, IOException, IteratorException, PinPageException,
            UnpinPageException, IteratorException {
        try {
            if ( trace !=null) {
                trace.writeBytes( "DELETE " + nid.pageNo + " " + nid.slotNo
                        + " " +  key +lineSep);
                trace.writeBytes("DO"+lineSep);
                trace.writeBytes( "SEARCH"+lineSep);
                trace.flush();
            }

            _Delete(key, nid, headerPage.get_rootId(), null);

            if ( trace !=null) {
                trace.writeBytes("DONE"+lineSep);
                trace.flush();
            }

            return true;
        } catch (RecordNotFoundException e) {
            return false;
        }
    }

    private KeyClass _Delete ( KeyClass key, NID nid, PageId currentPageId, PageId parentPageId)
            throws IndexInsertRecException, RedistributeException, IndexSearchException,
            RecordNotFoundException,
            DeleteRecException,
            InsertRecException,
            LeafRedistributeException,
            IndexFullDeleteException,
            FreePageException,
            LeafDeleteException,
            KeyNotMatchException,
            ConstructPageException,
            UnpinPageException,
            IteratorException,
            PinPageException,
            IOException {

        ZTSortedPage  sortPage;
        Page page;
        page=pinPage(currentPageId);
        sortPage = new ZTSortedPage(page, headerPage.get_keyType());

        if ( trace!=null ) {
            trace.writeBytes("VISIT node " + currentPageId +lineSep);
            trace.flush();
        }

        if (sortPage.getType()==NodeType.LEAF ) {
            NID curNid = new NID();  // iterator
            DescriptorKeyDataEntry tmpEntry;
            KeyClass curkey;
            NID dummyNid;
            PageId nextpage;
            ZTLeafPage leafPage;
            leafPage= new ZTLeafPage(page, headerPage.get_keyType());


            KeyClass deletedKey=key;
            tmpEntry = leafPage.getFirst(curNid);

            NID delNid;
            // for all records with key equal to 'key', delete it if its rid = 'rid'
            while((tmpEntry!=null) && (ZT.keyCompare(key,tmpEntry.key)>=0)) {
                // WriteUpdateLog is done in the btleafpage level - to log the
                // deletion of the rid.
                if ( leafPage.delEntry(new DescriptorKeyDataEntry(key, nid)) ) {
                    // successfully found <key, rid> on this page and deleted it.

                    if ( trace!=null ) {
                        trace.writeBytes("TAKEFROM node "+leafPage.getCurPage()+lineSep);
                        trace.flush();
                    }

                    PageId leafPage_no=leafPage.getCurPage();
                    if ( (4+leafPage.available_space()) <= ((MAX_SPACE-HFPage.DPFIXED)/2) ) {
                        // the leaf page is at least half full after the deletion
                        unpinPage(leafPage.getCurPage(), true /* = DIRTY */);
                        return null;
                    } else if (leafPage_no.pid == headerPage.get_rootId().pid) {
                        // the tree has only one node - the root
                        if (leafPage.numberOfRecords() != 0) {
                            unpinPage(leafPage_no, true /*= DIRTY */);
                            return null;
                        } else {
                            // the whole tree is empty
                            if ( trace!=null ) {
                                trace.writeBytes("DEALLOCATEROOT "
                                        + leafPage_no+lineSep);
                                trace.flush();
                            }
                            freePage(leafPage_no);
                            updateHeader(new PageId(INVALID_PAGE) );
                            return null;
                        }
                    } else {
                        // get a sibling
                        ZTIndexPage  parentPage;
                        parentPage = new ZTIndexPage(pinPage(parentPageId), headerPage.get_keyType());
                        PageId siblingPageId=new PageId();
                        ZTLeafPage siblingPage;
                        int direction;
                        direction = parentPage.getSibling(key, siblingPageId);

                        if (direction == 0) {
                            // there is no sibling. nothing can be done.
                            unpinPage(leafPage.getCurPage(), true /*=DIRTY*/);
                            unpinPage(parentPageId);
                            return null;
                        }

                        siblingPage = new ZTLeafPage(pinPage(siblingPageId), headerPage.get_keyType());

                        if (siblingPage.redistribute( leafPage, parentPage, direction, deletedKey)) {
                            // the redistribution has been done successfully
                            if ( trace!=null ) {
                                trace_children(leafPage.getCurPage());
                                trace_children(siblingPage.getCurPage());
                            }

                            unpinPage(leafPage.getCurPage(), true);
                            unpinPage(siblingPageId, true);
                            unpinPage(parentPageId, true);
                            return null;
                        } else if ( (siblingPage.available_space() + 8 /* 2*sizeof(slot) */ ) >=
                                ( (MAX_SPACE-HFPage.DPFIXED)
                                        - leafPage.available_space())) {

                            // we can merge these two children
                            // get old child entry in the parent first
                            DescriptorKeyDataEntry oldChildEntry;
                            if (direction==-1) {
                                oldChildEntry = leafPage.getFirst(curNid);
                            } else {
                                oldChildEntry=siblingPage.getFirst(curNid);
                            }

                            // merge the two children
                            ZTLeafPage leftChild;
                            ZTLeafPage rightChild;
                            if (direction==-1) {
                                leftChild = siblingPage;
                                rightChild = leafPage;
                            }
                            else {
                                leftChild = leafPage;
                                rightChild = siblingPage;
                            }

                            // move all entries from rightChild to leftChild
                            NID firstNid = new NID(), insertNid;
                            for (tmpEntry= rightChild.getFirst(firstNid);
                                 tmpEntry != null;
                                 tmpEntry=rightChild.getFirst(firstNid)) {
                                leftChild.insertRecord(tmpEntry);
                                rightChild.deleteSortedRecord(firstNid);
                            }

                            // adjust chain
                            leftChild.setNextPage(rightChild.getNextPage());
                            if ( rightChild.getNextPage().pid != INVALID_PAGE) {
                                ZTLeafPage nextLeafPage = new ZTLeafPage(
                                        rightChild.getNextPage(), headerPage.get_keyType());
                                nextLeafPage.setPrevPage(leftChild.getCurPage());
                                unpinPage( nextLeafPage.getCurPage(), true);
                            }

                            if ( trace!=null ) {
                                trace.writeBytes("MERGE nodes "+
                                        leftChild.getCurPage()
                                        + " " + rightChild.getCurPage()+lineSep);
                                trace.flush();
                            }

                            unpinPage(leftChild.getCurPage(), true);
                            unpinPage(parentPageId, true);
                            freePage(rightChild.getCurPage());
                            return  oldChildEntry.key;
                        } else {
                            // It's a very rare case when we can do neither
                            // redistribution nor merge.

                            unpinPage(leafPage.getCurPage(), true);
                            unpinPage(siblingPageId, true);
                            unpinPage(parentPageId, true);
                            return null;
                        }
                    } //get a sibling block
                }// delete success block

                nextpage = leafPage.getNextPage();
                unpinPage(leafPage.getCurPage());

                if (nextpage.pid == INVALID_PAGE )
                    throw  new RecordNotFoundException(null,"");

                leafPage = new ZTLeafPage(pinPage(nextpage), headerPage.get_keyType() );
                tmpEntry=leafPage.getFirst(curNid);

            } //while loop
	/*
	 * We reached a page with first key > `key', so return an error.
	 * We should have got true back from delUserRid above.  Apparently
	 * the specified <key,rid> data entry does not exist.
	 */

            unpinPage(leafPage.getCurPage());
            throw  new RecordNotFoundException(null,"");
        }


        if (  sortPage.getType() == NodeType.INDEX ) {
            PageId childPageId;
            ZTIndexPage indexPage = new ZTIndexPage(page, headerPage.get_keyType());
            childPageId = indexPage.getPageNoByKey(key);

            // now unpin the page, recurse and then pin it again
            unpinPage(currentPageId);

            KeyClass oldChildKey= _Delete(key, nid,  childPageId, currentPageId);

            // two cases:
            // - oldChildKey == null: one level lower no merge has occurred:
            // - oldChildKey != null: one of the children has been deleted and
            //                     oldChildEntry is the entry to be deleted.

            indexPage=new ZTIndexPage(pinPage(currentPageId), headerPage.get_keyType());

            if (oldChildKey ==null) {
                unpinPage(indexPage.getCurPage(),true);
                return null;
            }

            // delete the oldChildKey

            // save possible old child entry before deletion
            PageId dummyPageId;
            KeyClass deletedKey = key;
            NID curNid = indexPage.deleteKey(oldChildKey);

            if (indexPage.getCurPage().pid == headerPage.get_rootId().pid) {
                // the index page is the root
                if (indexPage.numberOfRecords() == 0) {
                    ZTSortedPage childPage;
                    childPage = new ZTSortedPage(indexPage.getPrevPage(),
                            headerPage.get_keyType());
                    if ( trace !=null ) {
                        trace.writeBytes( "CHANGEROOT from node " +
                                indexPage.getCurPage()
                                + " to node " +indexPage.getPrevPage()+lineSep);
                        trace.flush();
                    }

                    updateHeader(indexPage.getPrevPage());
                    unpinPage(childPage.getCurPage());
                    freePage(indexPage.getCurPage());
                    return null;
                }
                unpinPage(indexPage.getCurPage(),true);
                return null;
            }

            // now we know the current index page is not a root
            if ((4 /*sizeof slot*/ +indexPage.available_space()) <=
                    ((MAX_SPACE-HFPage.DPFIXED)/2)) {
                // the index page is at least half full after the deletion
                unpinPage(currentPageId,true);

                return null;
            } else {
                // get a sibling
                ZTIndexPage  parentPage;
                parentPage = new ZTIndexPage(pinPage(parentPageId),
                        headerPage.get_keyType());
                PageId siblingPageId=new PageId();
                ZTIndexPage siblingPage;
                int direction;
                direction=parentPage.getSibling(key,
                        siblingPageId);
                if ( direction==0) {
                    // there is no sibling. nothing can be done.
                    unpinPage(indexPage.getCurPage(), true);
                    unpinPage(parentPageId);
                    return null;
                }

                siblingPage = new ZTIndexPage( pinPage(siblingPageId), headerPage.get_keyType());
                int pushKeySize=0;
                if (direction==1) {
                    pushKeySize = ZT.getKeyLength
                            (parentPage.findKey(siblingPage.getFirst(new NID()).key));
                } else if (direction==-1) {
                    pushKeySize = ZT.getKeyLength
                            (parentPage.findKey(indexPage.getFirst(new NID()).key));
                }

                if (siblingPage.redistribute(indexPage,parentPage,
                        direction, deletedKey)) {
                    // the redistribution has been done successfully
                    if (trace!=null) {
                        trace_children(indexPage.getCurPage());
                        trace_children(siblingPage.getCurPage());
                    }
                    unpinPage(indexPage.getCurPage(), true);
                    unpinPage(siblingPageId, true);
                    unpinPage(parentPageId, true);
                    return null;
                } else if ( siblingPage.available_space()+4 /*slot size*/ >=
                        ((MAX_SPACE-HFPage.DPFIXED) -
                                (indexPage.available_space()+4 /*slot size*/)
                                +pushKeySize+4 /*slot size*/ + 4 /* pageId size*/)  ) {
                    // we can merge these two children
                    // get old child entry in the parent first
                    KeyClass oldChildEntry;
                    if (direction==-1) {
                        oldChildEntry=indexPage.getFirst(curNid).key;
                    } else {
                        oldChildEntry= siblingPage.getFirst(curNid).key;
                    }
                    // merge the two children
                    ZTIndexPage leftChild, rightChild;
                    if (direction==-1) {
                        leftChild = siblingPage;
                        rightChild = indexPage;
                    } else {
                        leftChild = indexPage;
                        rightChild = siblingPage;
                    }

                    if ( trace!= null ) {
                        trace.writeBytes( "MERGE nodes " + leftChild.getCurPage()
                                + " "
                                + rightChild.getCurPage()+lineSep);
                        trace.flush();
                    }

                    // pull down the entry in its parent node
                    // and put it at the end of the left child
                    NID firstNid = new NID(), insertNid;
                    PageId curPageId;

                    leftChild.insertKey( parentPage.findKey(oldChildEntry),
                            rightChild.getLeftLink());

                    // move all entries from rightChild to leftChild
                    for (DescriptorKeyDataEntry tmpEntry=rightChild.getFirst(firstNid);
                         tmpEntry != null;
                         tmpEntry=rightChild.getFirst(firstNid) ) {
                        leftChild.insertKey(tmpEntry.key,
                                ((IndexData)tmpEntry.data).getData());
                        rightChild.deleteSortedRecord(firstNid);
                    }

                    unpinPage(leftChild.getCurPage(), true);
                    unpinPage(parentPageId, true);
                    freePage(rightChild.getCurPage());
                    return oldChildEntry;  // ???
                } else {
                    // It's a very rare case when we can do neither
                    // redistribution nor merge.
                    unpinPage(indexPage.getCurPage(), true);
                    unpinPage(siblingPageId, true);
                    unpinPage(parentPageId);
                    return null;
                }
            }
        } //index node
        return null; //neither leaf and index page
    }

    public Descriptor getDescriptorFromBigInt(DescriptorKey bigInt)
    {
        Descriptor givenDesc=bigInt.getDesc();
        return givenDesc;
    }

    public boolean checkInRange(Descriptor upperLeft,Descriptor lowerRight,Descriptor current)
    {
        for(int i=0;i<5;i++){
            if(current.get(i)<upperLeft.get(i) || current.get(i)>lowerRight.get(i))
            {
                return false;
            }
        }
        return true;
    }


    public List<DescriptorKey> rangeScanBoundaries(KeyClass bigInt, int distance)
    {
        Descriptor givenDescriptor=getDescriptorFromBigInt((DescriptorKey) bigInt);
        short[] values=new short[5];
        for(int i=0;i<5;i++)
        {
            values[i]=(short)givenDescriptor.get(i);
        }
        short[] highValues=new short[5];
        short[] lowValues=new short[5];
        for(int i=0;i<5;i++)
        {
            highValues[i]=(short)(values[i]+distance);
            lowValues[i]=(short)(values[i]-distance);
            if(lowValues[i]<0)
                lowValues[i]=0;
        }
        Descriptor low_desc=new Descriptor();
        low_desc.set(lowValues[0],lowValues[1],lowValues[2],lowValues[3],lowValues[4]);
        Descriptor high_desc=new Descriptor();
        high_desc.set(highValues[0],highValues[1],highValues[2],highValues[3],highValues[4]);
        DescriptorKey low_key=new DescriptorKey(low_desc);
        DescriptorKey high_key=new DescriptorKey(high_desc);
        List<DescriptorKey> descKeyList=new ArrayList<>();
        descKeyList.add(low_key);
        descKeyList.add(high_key);
        return descKeyList;
    }

    public ZTFileScan new_scan(KeyClass lo_key, KeyClass hi_key)
            throws IOException,
            KeyNotMatchException,
            IteratorException,
            ConstructPageException,
            PinPageException,
            UnpinPageException {
        ZTFileScan scan = new ZTFileScan();
        if ( headerPage.get_rootId().pid==INVALID_PAGE) {
            scan.leafPage=null;
            return scan;
        }

        scan.treeFilename=dbname;
        scan.endkey=hi_key;
        scan.didfirst=false;
        scan.deletedcurrent=false;
        scan.curNid = new NID();
        scan.keyType=headerPage.get_keyType();
        scan.maxKeysize=headerPage.get_maxKeySize();
        scan.bfile=this;

        //this sets up scan at the starting position, ready for iteration
        scan.leafPage=findRunStart(lo_key, scan.curNid);
        return scan;
    }

    public void trace_children(PageId id)
            throws  IOException,
            IteratorException,
            ConstructPageException,
            PinPageException,
            UnpinPageException {

        if( trace!=null ) {
            ZTSortedPage sortedPage;
            NID metaNid=new NID();
            PageId childPageId;
            KeyClass key;
            DescriptorKeyDataEntry entry;
            sortedPage = new ZTSortedPage( pinPage( id), headerPage.get_keyType());

            // Now print all the child nodes of the page.
            if( sortedPage.getType()==NodeType.INDEX) {
                ZTIndexPage indexPage = new ZTIndexPage(sortedPage,headerPage.get_keyType());
                trace.writeBytes("INDEX CHILDREN " + id + " nodes" + lineSep);
                trace.writeBytes( " " + indexPage.getPrevPage());
                for (entry = indexPage.getFirst( metaNid );
                      entry != null; entry = indexPage.getNext(metaNid) ) {
                    trace.writeBytes( "   " + ((IndexData)entry.data).getData());
                }
            } else if ( sortedPage.getType()==NodeType.LEAF) {
                ZTLeafPage leafPage = new ZTLeafPage(sortedPage,headerPage.get_keyType());
                trace.writeBytes("LEAF CHILDREN " + id + " nodes" + lineSep);
                for ( entry = leafPage.getFirst( metaNid ); entry != null;
                      entry = leafPage.getNext( metaNid ) ) {
                    trace.writeBytes( "   " + entry.key + " " + entry.data);
                }
            }
            unpinPage( id );
            trace.writeBytes(lineSep);
            trace.flush();
        }
    }
}

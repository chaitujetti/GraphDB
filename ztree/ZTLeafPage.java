package ztree;

import diskmgr.Page;
import global.*;

import java.io.IOException;


public class ZTLeafPage extends ZTSortedPage {
    public ZTLeafPage(PageId pageno, int keyType) throws IOException, ConstructPageException {
        super(pageno, keyType);
        setType(NodeType.LEAF);
    }

    public ZTLeafPage(Page page, int keyType) throws IOException, ConstructPageException {
        super(page, keyType);
        setType(NodeType.LEAF);
    }

    public ZTLeafPage(int keyType) throws ConstructPageException, IOException {
        super(keyType);
        setType(NodeType.LEAF);
    }

    public NID insertRecord(KeyClass key, NID dataNid) throws LeafInsertRecException {
        DescriptorKeyDataEntry entry;
        try {
            entry = new DescriptorKeyDataEntry(key,dataNid);
            return insertRecord(entry);
        }
        catch(Exception e) {
            throw new LeafInsertRecException(e, "insert record failed");
        }
    } // end of insertRecord

    public DescriptorKeyDataEntry getFirst(NID nid) throws IteratorException {
        DescriptorKeyDataEntry entry;
        try {
            nid.pageNo = getCurPage();
            nid.slotNo = 0; // begin with first slot

            if ( getSlotCnt() <= 0) {
                return null;
            }

            entry = ZT.getEntryFromBytes(getpage(), getSlotOffset(0), getSlotLength(0),
                    keyType, NodeType.LEAF);

            return entry;
        }
        catch (Exception e) {
            throw new IteratorException(e, "Get first entry failed");
        }
    } // end of getFirst

    public DescriptorKeyDataEntry getNext (NID nid) throws  IteratorException {
        DescriptorKeyDataEntry entry;
        int i;
        try{
            nid.slotNo++; //must before any return;
            i = nid.slotNo;

            if (nid.slotNo >= getSlotCnt()) {
                return null;
            }

            entry = ZT.getEntryFromBytes(getpage(),getSlotOffset(i), getSlotLength(i),
                    keyType, NodeType.LEAF);

            return entry;
        }
        catch (Exception e) {
            throw new IteratorException(e,"Get next entry failed");
        }
    }

    public DescriptorKeyDataEntry getCurrent (NID nid) throws  IteratorException {
        nid.slotNo--;
        return getNext(nid);
    }

    public boolean delEntry (DescriptorKeyDataEntry dEntry) throws LeafDeleteException {
        DescriptorKeyDataEntry entry;
        NID nid = new NID();

        try {
            for(entry = getFirst(nid); entry!=null; entry=getNext(nid)) {
                if ( entry.equals(dEntry) ) {
                    if (!super.deleteSortedRecord(nid))
                        throw new LeafDeleteException(null, "Delete record failed");
                    return true;
                }

            }
            return false;
        }
        catch (Exception e) {
            throw new LeafDeleteException(e, "delete entry failed");
        }

    } // end of delEntry

    public boolean redistribute(ZTLeafPage leafPage, ZTIndexPage parentIndexPage,
                         int direction, KeyClass deletedKey) throws LeafRedistributeException {
        boolean st;
        // assertion: leafPage pinned
        try {
            if (direction ==-1) { // 'this' is the left sibling of leafPage
                if ( (getSlotLength(getSlotCnt()-1) + available_space()+ 8 /*  2*sizeof(slot) */) >
                        ((MAX_SPACE-DPFIXED)/2)) {
                    // cannot spare a record for its underflow sibling
                    return false;
                }
                else {
                    // move the last record to its sibling

                    // get the last record
                    DescriptorKeyDataEntry lastEntry;
                    lastEntry = ZT.getEntryFromBytes(getpage(),getSlotOffset(getSlotCnt()-1)
                            ,getSlotLength(getSlotCnt()-1), keyType, NodeType.LEAF);


                    //get its sibling's first record's key for adjusting parent pointer
                    NID dummyNid = new NID();
                    DescriptorKeyDataEntry firstEntry;
                    firstEntry = leafPage.getFirst(dummyNid);

                    // insert it into its sibling
                    leafPage.insertRecord(lastEntry);

                    // delete the last record from the old page
                    NID delNid = new NID();
                    delNid.pageNo = getCurPage();
                    delNid.slotNo = getSlotCnt() - 1;
                    if (!deleteSortedRecord(delNid)) {
                        throw new LeafRedistributeException(null, "delete record failed");
                    }

                    // adjust the entry pointing to sibling in its parent
                    if (deletedKey != null) {
                        st = parentIndexPage.adjustKey(lastEntry.key, deletedKey);
                    } else {
                        st = parentIndexPage.adjustKey(lastEntry.key,
                                firstEntry.key);
                    }
                    if (!st) {
                        throw new LeafRedistributeException(null, "adjust key failed");
                    }
                    return true;
                }
            } else { // 'this' is the right sibling of pptr
                if ( (getSlotLength(0) + available_space()+ 8) > ((MAX_SPACE-DPFIXED)/2)) {
                    // cannot spare a record for its underflow sibling
                    return false;
                } else {
                    // move the first record to its sibling

                    // get the first record
                    DescriptorKeyDataEntry firstEntry;
                    firstEntry = ZT.getEntryFromBytes(getpage(), getSlotOffset(0),
                            getSlotLength(0), keyType,
                            NodeType.LEAF);

                    // insert it into its sibling
                    NID dummyNid=new NID();
                    leafPage.insertRecord(firstEntry);


                    // delete the first record from the old page
                    NID delRid = new NID();
                    delRid.pageNo = getCurPage();
                    delRid.slotNo = 0;
                    if (!deleteSortedRecord(delRid)) {
                        throw new LeafRedistributeException(null, "delete record failed");
                    }

                    // get the current first record of the old page
                    // for adjusting parent pointer.
                    DescriptorKeyDataEntry tmpEntry;
                    tmpEntry = getFirst(dummyNid);

                    // adjust the entry pointing to itself in its parent
                    st = parentIndexPage.adjustKey(tmpEntry.key, firstEntry.key);
                    if(!st) {
                        throw new LeafRedistributeException(null, "adjust key failed");
                    }
                    return true;
                }
            }
        }
        catch (Exception e) {
            throw new LeafRedistributeException(e, "redistribute failed");
        }
    } // end of redistribute
}

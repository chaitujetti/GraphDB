/* File NHFPage.java */

package heap;

import java.io.*;
import java.lang.*;

import global.*;
import diskmgr.*;



/**
 * Define constant values for INVALID_SLOT and EMPTY_SLOT
 */

//interface ConstSlot{
  //  int INVALID_SLOT =  -1;
  //  int EMPTY_SLOT = -1;
//}

/** Class heap file page.
 * The design assumes that records are kept compacted when
 * deletions are performed. 
 */

public class NHFPage extends HFPage{

    public NHFPage ()   {  }

    /**
     * Constructor of class NHFPage
     * open a NHFPage and make this NHFpage piont to the given page
     * @param  page  the given page in Page type
     */

    public NHFPage(NHFPage page)
    {
        data = page.getpage();
    }

    /**
     * inserts a new record onto the page, returns NID of this record
     * @param	record 	a record to be inserted
     * @return	NID of record, null if sufficient space does not exist
     * @exception IOException I/O errors
     * in C++ Status insertRecord(char *recPtr, int recLen, NID& nid)
     */
    public NID insertNode ( byte [] record)
            throws IOException
    {
        NID nid = new NID();

        int recLen = record.length;
        int spaceNeeded = recLen + SIZE_OF_SLOT;

        // Start by checking if sufficient space exists.
        // This is an upper bound check. May not actually need a slot
        // if we can find an empty one.

        freeSpace = Convert.getShortValue (FREE_SPACE, data);
        if (spaceNeeded > freeSpace) {
            return null;

        } else {

            // look for an empty slot
            slotCnt = Convert.getShortValue (SLOT_CNT, data);
            int i;
            short length;
            for (i= 0; i < slotCnt; i++)
            {
                length = getSlotLength(i);
                if (length == EMPTY_SLOT)
                    break;
            }

            if(i == slotCnt)   //use a new slot
            {
                // adjust free space        
                freeSpace -= spaceNeeded;
                Convert.setShortValue (freeSpace, FREE_SPACE, data);

                slotCnt++;
                Convert.setShortValue (slotCnt, SLOT_CNT, data);

            }
            else {
                // reusing an existing slot
                freeSpace -= recLen;
                Convert.setShortValue (freeSpace, FREE_SPACE, data);
            }

            usedPtr = Convert.getShortValue (USED_PTR, data);
            usedPtr -= recLen;    // adjust usedPtr
            Convert.setShortValue (usedPtr, USED_PTR, data);

            //insert the slot info onto the data page
            setSlot(i, recLen, usedPtr);

            // insert data onto the data page
            System.arraycopy (record, 0, data, usedPtr, recLen);
            curPage.pid = Convert.getIntValue (CUR_PAGE, data);
            nid.pageNo.pid = curPage.pid;
            nid.slotNo = i;
            return   nid ;
        }
    }

    /**
     * delete the record with the specified nid
     * @param	nid 	the record ID
     * @exception	InvalidSlotNumberException Invalid slot number
     * @exception IOException I/O errors
     * in C++ Status deleteRecord(const NID& nid)
     */
    public void deleteNode ( NID nid )
            throws IOException,
            InvalidSlotNumberException
    {
        int slotNo = nid.slotNo;
        short recLen = getSlotLength (slotNo);
        slotCnt = Convert.getShortValue (SLOT_CNT, data);

        // first check if the record being deleted is actually valid
        if ((slotNo >= 0) && (slotNo < slotCnt) && (recLen > 0))
        {
            // The records always need to be compacted, as they are
            // not necessarily stored on the page in the order that
            // they are listed in the slot index.

            // offset of record being deleted
            int offset = getSlotOffset(slotNo);
            usedPtr = Convert.getShortValue (USED_PTR, data);
            int newSpot= usedPtr + recLen;
            int size = offset - usedPtr;

            // shift bytes to the right
            System.arraycopy(data, usedPtr, data, newSpot, size);

            // now need to adjust offsets of all valid slots that refer
            // to the left of the record being removed. (by the size of the hole)

            int i, n, chkoffset;
            for (i = 0, n = DPFIXED; i < slotCnt; n +=SIZE_OF_SLOT, i++) {
                if ((getSlotLength(i) >= 0))
                {
                    chkoffset = getSlotOffset(i);
                    if(chkoffset < offset)
                    {
                        chkoffset += recLen;
                        Convert.setShortValue((short)chkoffset, n+2, data);
                    }
                }
            }

            // move used Ptr forwar
            usedPtr += recLen;
            Convert.setShortValue (usedPtr, USED_PTR, data);

            // increase freespace by size of hole
            freeSpace = Convert.getShortValue(FREE_SPACE, data);
            freeSpace += recLen;
            Convert.setShortValue (freeSpace, FREE_SPACE, data);

            setSlot(slotNo, EMPTY_SLOT, 0);  // mark slot free
        }
        else {
            throw new InvalidSlotNumberException (null, "HEAPFILE: INVALID_SLOTNO");
        }
    }

    /**
     * @return NID of first record on page, null if page contains no records.  
     * @exception  IOException I/O errors
     * in C++ Status firstRecord(NID& firstNid)
     *
     */
    public NID firstNode()
            throws IOException
    {
        NID nid = new NID();
        // find the first non-empty slot


        slotCnt = Convert.getShortValue (SLOT_CNT, data);

        int i;
        short length;
        for (i= 0; i < slotCnt; i++)
        {
            length = getSlotLength (i);
            if (length != EMPTY_SLOT)
                break;
        }

        if(i== slotCnt)
            return null;

        // found a non-empty slot

        nid.slotNo = i;
        curPage.pid= Convert.getIntValue(CUR_PAGE, data);
        nid.pageNo.pid = curPage.pid;

        return nid;
    }

    /**
     * @return NID of next record on the page, null if no more
     * records exist on the page
     * @param 	curNid	current record ID
     * @exception  IOException I/O errors
     * in C++ Status nextRecord (NID curNid, NID& nextNid)
     */
    public NID nextNode (NID curNid)
            throws IOException
    {
        NID nid = new NID();
        slotCnt = Convert.getShortValue (SLOT_CNT, data);

        int i=curNid.slotNo;
        short length;

        // find the next non-empty slot
        for (i++; i < slotCnt;  i++)
        {
            length = getSlotLength(i);
            if (length != EMPTY_SLOT)
                break;
        }

        if(i >= slotCnt)
            return null;

        // found a non-empty slot

        nid.slotNo = i;
        curPage.pid = Convert.getIntValue(CUR_PAGE, data);
        nid.pageNo.pid = curPage.pid;

        return nid;
    }

    /**
     * copies out record with NID nid into record pointer.
     * <br>
     * Status getRecord(NID nid, char *recPtr, int& recLen)
     * @param	nid 	the record ID
     * @return 	a tuple contains the record
     * @exception   InvalidSlotNumberException Invalid slot number
     * @exception  	IOException I/O errors
     * @see 	Tuple
     */
    public Node getNode ( NID nid )
            throws IOException, InvalidTupleSizeException,
            InvalidSlotNumberException, InvalidTypeException
    {
        short recLen;
        short offset;
        byte []record;
        PageId pageNo = new PageId();
        pageNo.pid= nid.pageNo.pid;
        curPage.pid = Convert.getIntValue (CUR_PAGE, data);
        int slotNo = nid.slotNo;

        // length of record being returned
        recLen = getSlotLength (slotNo);
        slotCnt = Convert.getShortValue (SLOT_CNT, data);
        if (( slotNo >=0) && (slotNo < slotCnt) && (recLen >0)
                && (pageNo.pid == curPage.pid))
        {
            offset = getSlotOffset (slotNo);
            record = new byte[recLen];
            System.arraycopy(data, offset, record, 0, recLen);
            Node tuple = new Node(record, 0, recLen);
            return tuple;
        }

        else {
            throw new InvalidSlotNumberException (null, "HEAPFILE: INVALID_SLOTNO");
        }


    }

    /**
     * returns a tuple in a byte array[pageSize] with given NID nid.
     * <br>
     * in C++	Status returnRecord(NID nid, char*& recPtr, int& recLen)
     * @param       nid     the record ID
     * @return      a tuple  with its length and offset in the byte array
     * @exception   InvalidSlotNumberException Invalid slot number
     * @exception   IOException I/O errors
     * @see 	Tuple
     */
    public Node returnNode ( NID nid )
            throws IOException, InvalidTupleSizeException,
            InvalidSlotNumberException, InvalidTypeException
    {
        short recLen;
        short offset;
        PageId pageNo = new PageId();
        pageNo.pid = nid.pageNo.pid;

        curPage.pid = Convert.getIntValue (CUR_PAGE, data);
        int slotNo = nid.slotNo;

        // length of record being returned
        recLen = getSlotLength (slotNo);
        slotCnt = Convert.getShortValue (SLOT_CNT, data);

        if (( slotNo >=0) && (slotNo < slotCnt) && (recLen >0)
                && (pageNo.pid == curPage.pid))
        {

            offset = getSlotOffset (slotNo);
            Node tuple = new Node(data, offset, recLen);
            return tuple;
        }

        else {
            throw new InvalidSlotNumberException (null, "HEAPFILE: INVALID_SLOTNO");
        }

    }

}

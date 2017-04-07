package heap;

import java.io.*;
import diskmgr.*;
import bufmgr.*;
import global.*;

/**
 * Created by vamsikrishnag on 3/6/17.
 */

public class NodeHeapfile extends Heapfile {

    /**
     * Initialize.  A null name produces a temporary heapfile which will be
     * deleted by the destructor.  If the name already denotes a file, the
     * file is opened; otherwise, a new empty file is created.
     *
     * @throws HFException        heapfile exception
     * @throws HFBufMgrException  exception thrown from bufmgr layer
     * @throws HFDiskMgrException exception thrown from diskmgr layer
     * @throws IOException        I/O errors
     */
    public NodeHeapfile(String name)
            throws HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException

    {
        super(name);
    }

    /**
     * Return number of records in file.
     *
     * @throws InvalidSlotNumberException invalid slot number
     * @throws InvalidTupleSizeException  invalid tuple size
     * @throws HFBufMgrException          exception thrown from bufmgr layer
     * @throws HFDiskMgrException         exception thrown from diskmgr layer
     * @throws IOException                I/O errors
     */
    public int getNodeCnt()
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFDiskMgrException,
            HFBufMgrException,
            IOException

    {
        return super.getRecCnt();
    } // end of getRecCnt


    /**
     * Insert record into file, return its Nid.
     *
     * @return the nid of the record
     * @throws InvalidSlotNumberException invalid slot number
     * @throws InvalidTupleSizeException  invalid tuple size
     * @throws SpaceNotAvailableException no space left
     * @throws HFException                heapfile exception
     * @throws HFBufMgrException          exception thrown from bufmgr layer
     * @throws HFDiskMgrException         exception thrown from diskmgr layer
     * @throws IOException                I/O errors
     */
    public NID insertNode(byte[] recPtr)
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            SpaceNotAvailableException,
            HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException {
        RID recordId = super.insertRecord(recPtr);
        NID nodeId = new NID(recordId.pageNo, recordId.slotNo);
        return nodeId;
    }

    /**
     * Delete record from file with given nid.
     *
     * @return true record deleted  false:record not found
     * @throws InvalidSlotNumberException invalid slot number
     * @throws InvalidTupleSizeException  invalid tuple size
     * @throws HFException                heapfile exception
     * @throws HFBufMgrException          exception thrown from bufmgr layer
     * @throws HFDiskMgrException         exception thrown from diskmgr layer
     * @throws Exception                  other exception
     */
    public boolean deleteNode(NID nid)
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFException,
            HFBufMgrException,
            HFDiskMgrException,
            Exception

    {
        return super.deleteRecord(nid);
    }


    /**
     * Updates the specified record in the heapfile.
     *
     * @param nid:      the record which needs update
     * @param newtuple: the new content of the record
     * @return ture:update success   false: can't find the record
     * @throws InvalidSlotNumberException invalid slot number
     * @throws InvalidUpdateException     invalid update on record
     * @throws InvalidTupleSizeException  invalid tuple size
     * @throws HFException                heapfile exception
     * @throws HFBufMgrException          exception thrown from bufmgr layer
     * @throws HFDiskMgrException         exception thrown from diskmgr layer
     * @throws Exception                  other exception
     */
    public boolean updateNode(NID nid, Node newtuple)
            throws InvalidSlotNumberException,
            InvalidUpdateException,
            InvalidTupleSizeException,
            HFException,
            HFDiskMgrException,
            HFBufMgrException,
            Exception {
        return updateRecord(nid, newtuple);
    }


    /**
     * Read record from file, returning pointer and length.
     *
     * @param nid Record ID
     * @return a Tuple. if Tuple==null, no more tuple
     * @throws InvalidSlotNumberException invalid slot number
     * @throws InvalidTupleSizeException  invalid tuple size
     * @throws SpaceNotAvailableException no space left
     * @throws HFException                heapfile exception
     * @throws HFBufMgrException          exception thrown from bufmgr layer
     * @throws HFDiskMgrException         exception thrown from diskmgr layer
     * @throws Exception                  other exception
     */
    public Node getNode(NID nid)
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFException,
            HFDiskMgrException,
            HFBufMgrException,
            Exception {

        RID record = new RID(nid.pageNo, nid.slotNo);
        Tuple tuple = super.getRecord(record);
        if (tuple != null) {
            Node newNode = new Node(tuple.data, 0);
            Descriptor descriptor = Convert.getDescValue(10, tuple.data);
            String label = Convert.getStrValue(0, tuple.data, 10);
            newNode.setDesc(descriptor);
            newNode.setLabel(label);
            return newNode;
        } else
            return null;
    }

    /**
     * Initiate a sequential scan.
     *
     * @throws InvalidTupleSizeException Invalid tuple size
     * @throws IOException               I/O errors
     */
    public NScan openScan()
            throws InvalidTupleSizeException,
            IOException {
        NScan newscan = new NScan(this);
        return newscan;
    }

    public String getFilename()
    {
        return _fileName;
    }


}
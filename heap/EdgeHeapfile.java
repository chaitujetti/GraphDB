package heap;


/**
 * Created by vamsikrishnag on 3/7/17.
 */


import diskmgr.Page;
import global.GlobalConst;
import global.RID;
import global.EID;
import global.PageId;
import global.SystemDefs;
import global.NID;
import java.io.IOException;
import global.Convert;
import java.io.*;
import java.util.HashSet;


public class EdgeHeapfile extends Heapfile {

    /** Initialize.  A null name produces a temporary heapfile which will be
     * deleted by the destructor.  If the name already denotes a file, the
     * file is opened; otherwise, a new empty file is created.
     *
     * @exception HFException heapfile exception
     * @exception HFBufMgrException exception thrown from bufmgr layer
     * @exception HFDiskMgrException exception thrown from diskmgr layer
     * @exception IOException I/O errors
     */
    public  EdgeHeapfile(String name)
            throws HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException

    {
        super(name);

    } // end of constructor

    /** Return number of records in file.
     *
     * @exception InvalidSlotNumberException invalid slot number
     * @exception InvalidTupleSizeException invalid tuple size
     * @exception HFBufMgrException exception thrown from bufmgr layer
     * @exception HFDiskMgrException exception thrown from diskmgr layer
     * @exception IOException I/O errors
     */
    public int getEdgeCnt()
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFDiskMgrException,
            HFBufMgrException,
            IOException

    {
        return super.getRecCnt();
    } // end of getRecCnt

    /** Insert record into file, return its Nid.
     *
     * @param recPtr pointer of the record
     *
     * @exception InvalidSlotNumberException invalid slot number
     * @exception InvalidTupleSizeException invalid tuple size
     * @exception SpaceNotAvailableException no space left
     * @exception HFException heapfile exception
     * @exception HFBufMgrException exception thrown from bufmgr layer
     * @exception HFDiskMgrException exception thrown from diskmgr layer
     * @exception IOException I/O errors
     *
     * @return the nid of the record
     */
    public EID insertEdge(byte[] recPtr)
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            SpaceNotAvailableException,
            HFException,
            HFBufMgrException,
            HFDiskMgrException,
            IOException
    {
        RID recordid = super.insertRecord(recPtr);
        EID eid = new EID(recordid.pageNo,recordid.slotNo);
        return eid;

    }

    /** Delete record from file with given nid.
     *
     * @exception InvalidSlotNumberException invalid slot number
     * @exception InvalidTupleSizeException invalid tuple size
     * @exception HFException heapfile exception
     * @exception HFBufMgrException exception thrown from bufmgr layer
     * @exception HFDiskMgrException exception thrown from diskmgr layer
     * @exception Exception other exception
     *
     * @return true record deleted  false:record not found
     */
    public boolean deleteEdge(EID eid)
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFException,
            HFBufMgrException,
            HFDiskMgrException,
            Exception

    {
        return super.deleteRecord(eid);
    }


    /** Updates the specified record in the heapfile.
     * @param eid: the record which needs update
     * @param newtuple: the new content of the record
     *
     * @exception InvalidSlotNumberException invalid slot number
     * @exception InvalidUpdateException invalid update on record
     * @exception InvalidTupleSizeException invalid tuple size
     * @exception HFException heapfile exception
     * @exception HFBufMgrException exception thrown from bufmgr layer
     * @exception HFDiskMgrException exception thrown from diskmgr layer
     * @exception Exception other exception
     * @return ture:update success   false: can't find the record
     */
    public boolean updateEdge(EID eid, Edge newtuple)
            throws InvalidSlotNumberException,
            InvalidUpdateException,
            InvalidTupleSizeException,
            HFException,
            HFDiskMgrException,
            HFBufMgrException,
            Exception
    {
        return updateRecord(eid,newtuple);
    }


    /** Read record from file, returning pointer and length.
     * @param eid Record ID
     *
     * @exception InvalidSlotNumberException invalid slot number
     * @exception InvalidTupleSizeException invalid tuple size
     * @exception SpaceNotAvailableException no space left
     * @exception HFException heapfile exception
     * @exception HFBufMgrException exception thrown from bufmgr layer
     * @exception HFDiskMgrException exception thrown from diskmgr layer
     * @exception Exception other exception
     *
     * @return a Tuple. if Tuple==null, no more tuple
     */
    public  Edge getEdge(EID eid)
            throws InvalidSlotNumberException,
            InvalidTupleSizeException,
            HFException,
            HFDiskMgrException,
            HFBufMgrException,
            Exception
    {
        Tuple temp = super.getRecord(eid);
        if (temp != null)
        {
            Edge edge = new Edge(temp.data, 0);
            String edgelabel = Convert.getStrValue(0, edge.data, 10);

            NID source = new NID();
            source.pageNo.pid = Convert.getIntValue(10, edge.data);
            source.slotNo=Convert.getIntValue(14, edge.data);

            NID destination = new NID();
            destination.pageNo.pid = Convert.getIntValue(18, edge.data);
            destination.slotNo=Convert.getIntValue(22, edge.data);

            int weight=Convert.getIntValue(26, edge.data);

            edge.setLabel(edgelabel);
            edge.setSource(source);
            edge.setDestination(destination);
            edge.setWeight(weight);

            return edge;
        }else{
            return null;
        }
    }

    public EScan openScan() throws InvalidTupleSizeException, IOException {
        EScan newscan = new EScan(this);
        return newscan;
    }

    public String getFileName() {
        return _fileName;
    }

}

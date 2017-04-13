package heap;

/**
 * Created by vamsikrishnag on 3/7/17.
 */

import java.io.*;
import global.*;
import bufmgr.*;
import diskmgr.*;


/**
 * A Scan object is created ONLY through the function openScan
 * of a HeapFile. It supports the getNext interface which will
 * simply retrieve the next record in the heapfile.
 *
 * An object of type scan will always have pinned one directory page
 * of the heapfile.
 */
public class EScan extends Scan{


    /** The constructor pins the first directory page in the file
     * and initializes its private data members from the private
     * data member from hf
     *
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     *
     * @param hf A HeapFile object
     */
    public EScan(Heapfile hf)
            throws InvalidTupleSizeException,
            IOException
    {
        super(hf);
    }

    /** Retrieve the next record in a sequential scan
     *
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     *
     * @param eid Record ID of the record
     * @return the Tuple of the retrieved record.
     */
    public Edge getNext(EID eid)
            throws InvalidTupleSizeException,
            IOException, InvalidTypeException
    {
        RID rid = new RID(eid.pageNo,eid.slotNo);
        Tuple tuple = super.getNext(rid);
        eid.pageNo.pid=rid.pageNo.pid;
        eid.slotNo=rid.slotNo;
        if(tuple!=null){
            Edge edge = new Edge(tuple.data, 0);
            return edge;
        } else {
            return null;
        }
    }

    /** Position the scan cursor to the record with the given rid.
     *
     * @exception InvalidTupleSizeException Invalid tuple size
     * @exception IOException I/O errors
     * @param eid Record ID of the given record
     * @return 	true if successful,
     *			false otherwise.
     */
    public boolean position(EID eid)
            throws InvalidTupleSizeException,
            IOException
    {
        return position(eid);
    }

}


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
public class NScan extends Scan {

    /**
     * The constructor pins the first directory page in the file
     * and initializes its private data members from the private
     * data member from hf
     *
     * @param hf A HeapFile object
     * @throws InvalidTupleSizeException Invalid tuple size
     * @throws IOException               I/O errors
     */
    public NScan(Heapfile hf)
            throws InvalidTupleSizeException,
            IOException {
        super(hf);
    }

    /**
     * Retrieve the next record in a sequential scan
     *
     * @param nid Record ID of the record
     * @return the Tuple of the retrieved record.
     * @throws InvalidTupleSizeException Invalid tuple size
     * @throws IOException               I/O errors
     */
    public Node getNext(NID nid)
            throws InvalidTupleSizeException,
            IOException, FieldNumberOutOfBoundException {
        RID record = new RID(nid.pageNo, nid.slotNo);
        Tuple tuple = super.getNext(record);
        nid.pageNo.pid = record.pageNo.pid;
        nid.slotNo = record.slotNo;
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
     * Position the scan cursor to the record with the given rid.
     *
     * @param nid Record ID of the given record
     * @return true if successful,
     * false otherwise.
     * @throws InvalidTupleSizeException Invalid tuple size
     * @throws IOException               I/O errors
     */
    public boolean position(NID nid)
            throws InvalidTupleSizeException,
            IOException {
        return position(nid);
    }
}

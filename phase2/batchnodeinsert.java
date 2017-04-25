package phase2;

import diskmgr.*;
import bufmgr.*;
import btree.*;
import heap.*;

import global.Descriptor;
import global.GlobalConst;
import global.NID;
import global.SystemDefs;
import tests.TestDriver;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by vamsikrishnag on 3/11/17.
 */
public class batchnodeinsert extends TestDriver implements GlobalConst
{

    public batchnodeinsert() { }


    static boolean nodeinsert(String filename,String dbname,SystemDefs systemdef)
            throws IOException,
            InvalidPageNumberException,
            FileIOException,
            DiskMgrException,
            FieldNumberOutOfBoundException,
            HFException,HFBufMgrException,HFDiskMgrException,
            InvalidSlotNumberException,InvalidTupleSizeException,SpaceNotAvailableException,btree.AddFileEntryException,btree.GetFileEntryException,
            btree.ConstructPageException,ztree.GetFileEntryException,ztree.AddFileEntryException,ztree.ConstructPageException,Exception
    {
        
        int counter = 0;
        List<String> content = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String lineread;
            while ((lineread = br.readLine()) != null) {
                content.add(lineread);
            }
        }

        Node iterator = new Node();
        Descriptor desc = new Descriptor();

        for(int i=0;i<content.size();i++)
        {
            String[] temp = content.get(i).split(" ");
            if(!checkNodeExists(temp[0], systemdef)){
                desc.set(Short.valueOf(temp[1]),Short.valueOf(temp[2]),Short.valueOf(temp[3]),Short.valueOf(temp[4]),Short.valueOf(temp[5]));

                iterator.setLabel(temp[0]);
                iterator.setDesc(desc);
                systemdef.JavabaseDB.insertNodeIntoGraphDB(iterator.getTupleByteArray());
            } else {
                System.out.println("Existing Node: " + temp[0]);
            }
            
            counter++;
        }
        
        System.out.println("Node count = " + systemdef.JavabaseDB.getNodeCnt());
        System.out.println("Edge count = " + systemdef.JavabaseDB.getEdgeCnt());
        System.out.println("Disk pages read =" + systemdef.JavabaseDB.getNoOfReads());
        System.out.println("Disk pages written =" + systemdef.JavabaseDB.getNoOfWrites());
        systemdef.JavabaseDB.flushCounters();

        if(counter == content.size())
            return true;
        else return false;
    }

    public static boolean checkNodeExists(String node, SystemDefs systemdef)
        throws IOException, InvalidTupleSizeException, InvalidTypeException,
        FieldNumberOutOfBoundException, ScanIteratorException, KeyNotMatchException, 
        IteratorException, ConstructPageException, PinPageException, UnpinPageException,
        InvalidFrameNumberException, ReplacerException, PageUnpinnedException,
        HashEntryNotFoundException
    {
        BTFileScan nodeScan = systemdef.JavabaseDB.getNodeIndex().new_scan(new StringKey(node), new StringKey(node));
        boolean nodeExists = (nodeScan.get_next() != null);
        nodeScan.DestroyBTreeFileScan();
        return nodeExists;
    }
}

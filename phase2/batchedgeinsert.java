package phase2;

/**
 * Created by joelmascarenhas on 3/11/17.
 */

import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import btree.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

public class batchedgeinsert implements GlobalConst
{

    public batchedgeinsert() { }

    static boolean edgeinsert(String filename,String dbname,SystemDefs systemdef)
            throws IOException,
            InvalidPageNumberException,
            FileIOException,
            DiskMgrException,
            FieldNumberOutOfBoundException,
            HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException, SpaceNotAvailableException, InvalidTupleSizeException,Exception {
        
        int counter = 0;
        List<String> content = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String lineread;
            while ((lineread = br.readLine()) != null) {
                content.add(lineread);
            }
        }


        for (int i=0;i<content.size();i++)
        {
            Edge iter = new Edge();
            NID dest_nid = new NID();
            NID source_nid = new NID();
            String[] temp = content.get(i).split(" ");

            source_nid = getNID(temp[0], systemdef);
            dest_nid = getNID(temp[1], systemdef);
            if(source_nid == null){
                System.out.println("Source Node does not exist");
            } else if(dest_nid == null){
                System.out.println("Destination Node does not exist");
            } else {
                iter.setSource(source_nid);
                iter.setDestination(dest_nid);
                iter.setSourceLabel(temp[0]);
                iter.setDestinationLabel(temp[1]);
                iter.setLabel(temp[2]);
                iter.setWeight(Integer.parseInt(temp[3]));
                systemdef.JavabaseDB.insertEdgeIntoGraphDB(iter.getTupleByteArray());
            }
            // else if(checkEdgeExists(temp[0], temp[1], temp[2], systemdef)) {
            //     System.out.println("Edge already exists: " + temp[2]);
            // } 
            
            counter++;
        }

        System.out.println("Node count = "+ systemdef.JavabaseDB.getNodeCnt());
        System.out.println("Edge count = "+ systemdef.JavabaseDB.getEdgeCnt());
        System.out.println("Disk pages read ="+ systemdef.JavabaseDB.getNoOfReads());
        System.out.println("Disk pages written ="+ systemdef.JavabaseDB.getNoOfWrites());
        systemdef.JavabaseDB.flushCounters();
        
        if(counter == content.size())
            return true;
        else 
            return false;
    }

    public static boolean checkEdgeExists(String source, String destination, String edgeLabel, SystemDefs systemdef)
        throws IOException, InvalidTupleSizeException, InvalidTypeException,
        FieldNumberOutOfBoundException, ScanIteratorException, KeyNotMatchException, 
        IteratorException, ConstructPageException, PinPageException, UnpinPageException,
        InvalidFrameNumberException, ReplacerException, PageUnpinnedException,
        HashEntryNotFoundException
    {
        BTFileScan edgeScan = systemdef.JavabaseDB.getEdgeIndex().new_scan(new StringKey(edgeLabel), new StringKey(edgeLabel));
        boolean edgeExists = (edgeScan.get_next() != null);
        edgeScan.DestroyBTreeFileScan();
        // BTFileScan edgeScan = systemdef.JavabaseDB.getEdgeSourceIndex().new_scan(new StringKey(source), new StringKey(source)).get_next();
        // BTFileScan edgeScan = systemdef.JavabaseDB.getEdgeDestinationIndex().new_scan(new StringKey(destination), new StringKey(destination)).get_next();
        // BTFileScan edgeScan = systemdef.JavabaseDB.getEdgeIndex().new_scan(new StringKey(edgeLabel), new StringKey(edgeLabel));
        // KeyDataEntry entry = edgeScan.get_next();
        // edgeScan.DestroyBTreeFileScan();
        // EID edgeId = new EID();
        // boolean edgeExists = false;
        // if(entry != null) {
        //     LeafData leafNode=(LeafData)entry.data;
        //     RID record = leafNode.getData();
        //     edgeId.pageNo.pid = record.pageNo.pid;
        //     edgeId.slotNo = record.slotNo;
        //     Edge edge=systemdef.JavabaseDB.getEhf().getEdge(edgeId);
        //     if(edge.getLabel() == edgeLabel && edge.getSourceLabel() == source && edge.getDestinationLabel() == destination){
        //         edgeExists = true;
        //     }
        // }
        // return edgeExists;
        // return (edgeScan.get_next() != null);

        return edgeExists;
    }

    public static NID getNID(String node, SystemDefs systemdef)
        throws IOException, InvalidTupleSizeException, InvalidTypeException,
        FieldNumberOutOfBoundException, ScanIteratorException, KeyNotMatchException, 
        IteratorException, ConstructPageException, PinPageException, UnpinPageException,
        InvalidFrameNumberException, ReplacerException, PageUnpinnedException,
        HashEntryNotFoundException
    {
        BTFileScan nodescan = systemdef.JavabaseDB.getNodeIndex().new_scan(new StringKey(node), new StringKey(node));
        KeyDataEntry entry = nodescan.get_next();
        nodescan.DestroyBTreeFileScan();

        NID nodeId = new NID();
        if(entry != null) {
            LeafData leafNode=(LeafData)entry.data;
            RID record = leafNode.getData();
            nodeId.pageNo.pid = record.pageNo.pid;
            nodeId.slotNo = record.slotNo;
        }
        
        return nodeId;
    }
}


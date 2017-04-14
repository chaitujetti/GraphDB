package phase2;

/**
 * Created by joelmascarenhas on 3/11/17.
 */

import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;

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
            NScan nodescan = systemdef.JavabaseDB.getNhf().openScan();
            NID dest_nid = new NID();
            NID source_nid = new NID();
            Node current_node;
            String[] temp = content.get(i).split(" ");

            source_nid = getNID(temp[0], systemdef);
            dest_nid = getNID(temp[1], systemdef);
            if(source_nid == null){
                System.out.println("Source Node does not exist");
            } else if(dest_nid == null){
                System.out.println("Destination Node does not exist");
            } else if(checkEdgeExists(temp[0], temp[1], temp[2], systemdef)) {
                System.out.println("Edge already exists: " + temp[2]);
            } else {
                iter.setSource(source_nid);
                iter.setDestination(dest_nid);
                iter.setSourceLabel(temp[0]);
                iter.setDestinationLabel(temp[1]);
                iter.setLabel(temp[2]);
                iter.setWeight(Integer.parseInt(temp[3]));
                systemdef.JavabaseDB.insertEdgeIntoGraphDB(iter.getTupleByteArray());
            }
            
            counter++;
        }

        System.out.println("Node count = "+ systemdef.JavabaseDB.getNodeCnt());
        System.out.println("Edge count = "+ systemdef.JavabaseDB.getEdgeCnt());
        System.out.println("Disk pages read ="+ systemdef.JavabaseDB.getNoOfReads());
        System.out.println("Disk pages written ="+ systemdef.JavabaseDB.getNoOfWrites());
        System.out.println("Unique labels in the file ="+ systemdef.JavabaseDB.getLabelCnt());

        if(counter == content.size())
            return true;
        else 
            return false;
    }

    public static NID getNID(String node, SystemDefs systemdef)
        throws IOException, InvalidTupleSizeException, InvalidTypeException, FieldNumberOutOfBoundException
    {
        NScan nodescan = systemdef.JavabaseDB.getNhf().openScan();
        NID start_nid = new NID();
        NID nodeId = new NID();
        Node current_node;
        boolean existingNode = false;

        while (!existingNode) {
            current_node = nodescan.getNext(start_nid);
            if(current_node==null){
                break;
            }
            if (current_node.getLabel().equals(node)) {
                existingNode = true;
                nodeId = new NID(new PageId(start_nid.pageNo.pid), start_nid.slotNo);
                nodescan.closescan();
            }
        }

        return nodeId;
    }

    public static boolean checkEdgeExists(String source, String destination, String edgeLabel, SystemDefs systemdef)
        throws IOException, InvalidTupleSizeException, InvalidTypeException, FieldNumberOutOfBoundException
    {
        EScan edgescan = systemdef.JavabaseDB.getEhf().openScan();
        EID start_eid = new EID();
        Edge current_edge = new Edge();
        boolean existingEdge = false;

        while (!existingEdge) {
            current_edge = edgescan.getNext(start_eid);
            if(current_edge == null){
                break;
            }
            if (current_edge.getLabel().equals(edgeLabel) && current_edge.getSourceLabel().equals(source) && current_edge.getDestinationLabel().equals(destination)) {
                existingEdge = true;
                edgescan.closescan();
            }
        }

        return existingEdge;
    }
}


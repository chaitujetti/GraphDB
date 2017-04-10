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
        System.out.println(filename);
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
            NID start_nid = new NID();
            NID dest_nid = new NID();
            NID source_nid = new NID();
            Node current_node;
            boolean source_present = false;
            boolean dest_present = false;
            String[] temp = content.get(i).split(" ");

            while((!source_present) || (!dest_present))
            {
                current_node = nodescan.getNext(start_nid);
                if(current_node.getLabel().equals(temp[0]))
                {
                    source_present = true;
                    source_nid = new NID(new PageId(start_nid.pageNo.pid), start_nid.slotNo);
                }
                if(current_node.getLabel().equals(temp[1]))
                {
                    dest_present = true;
                    dest_nid = new NID(new PageId(start_nid.pageNo.pid), start_nid.slotNo);
                }
            }
            if(source_present == false)
                System.out.println("Source Node not present");
            if(dest_present == false)
                System.out.println("Destination Node not present");

            EID eid = new EID();
            
            iter.setSource(source_nid);
            iter.setDestination(dest_nid);
            iter.setLabel(temp[2]);
            iter.setWeight(Integer.parseInt(temp[3]));
            byte[] tempiter = iter.getTupleByteArray();
            Edge ed=new Edge(tempiter,0);
            systemdef.JavabaseDB.insertEdgeIntoGraphDB(ed.getTupleByteArray());
            //eid = phase2.getEhf().insertEdge(tempiter);
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
}


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
        int[] res = new int[]{0,0,0,0,0};
//        GraphDB phase2 = new GraphDB();
//        phase2.openDB(dbname,1000);

//        SystemDefs systemdef=new SystemDefs(dbname,1000,256,"Clock");
//        systemdef.JavabaseDB.createIndexFiles(dbname);

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

        // get the node count
        res[0] = systemdef.JavabaseDB.getNodeCnt();

        // get the edge count
        res[1] = systemdef.JavabaseDB.getEdgeCnt();

        // get the pages read count
        res[2] = systemdef.JavabaseDB.getNoOfReads();
        // PCounter.getRcounter();

        //get the pages write count
        res[3] = systemdef.JavabaseDB.getNoOfWrites();

        //get node labels count
        res[4]=systemdef.JavabaseDB.getLabelCnt();


        System.out.println("Node count = "+ res[0]);
        System.out.println("Edge count = "+ res[1]);
        System.out.println("Disk pages read ="+ res[2]);
        System.out.println("Disk pages written ="+ res[3]);
        System.out.println("Unique labels in the file ="+ res[4]);

        if(counter == content.size())
            return true;
        else return false;
    }
}


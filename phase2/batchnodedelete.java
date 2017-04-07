package phase2;

import diskmgr.*;
import global.*;
import heap.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

/**
 * Created by joelmascarenhas on 3/11/17.
 */
public class batchnodedelete implements GlobalConst{
    static boolean nodedelete(String filename, String dbname, SystemDefs systemdef)
            throws Exception {
        int[] res = new int[]{0,0,0,0,0,0};
        int counter = 0;
        System.out.println(filename);
        List<String> content = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String lineread;
            while ((lineread = br.readLine()) != null) {
                content.add(lineread);
            }
        }

        for (int i=0;i<content.size();i++) {
            NScan nodescan = systemdef.JavabaseDB.getNhf().openScan();
            NID start_nid = new NID();
            NID delnid = new NID();
            Node current_node;
            boolean delnode = false;

            String temp = content.get(i);
            while (!delnode) {
                current_node = nodescan.getNext(start_nid);
                if (current_node.getLabel().equals(temp)) {
                    delnode = true;
                    delnid.copyRid(start_nid);
                }
            }

            EScan edgescan = systemdef.JavabaseDB.getEhf().openScan();
            EID start_eid = new EID();
            EID deleid = new EID();
            List<EID> edgesToBeDeleted = new ArrayList<>();
            Edge current_edge;
            boolean edgedelstatus;
            current_edge = edgescan.getNext(start_eid);
            NID src = new NID();
            Node sourcenode;
            Node destnode;

            NID dest = new NID();
            while(current_edge != null)
            {
                if(current_edge.getSource().equals(delnid) || current_edge.getDestination().equals(delnid)){
                    deleid.copyRid(start_eid);
                    edgedelstatus = systemdef.JavabaseDB.deleteEdgeFromGraphDB(deleid);
                }
                current_edge = edgescan.getNext(start_eid);
            }
            
            boolean stat = systemdef.JavabaseDB.deleteNodeFromGraphDB(delnid);
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

        res[5]=systemdef.JavabaseDB.getLabelCnt();

        System.out.println("Node count = " + res[0]);
        System.out.println("Edge count = " + res[1]);
        System.out.println("Disk pages read =" + res[2]);
        System.out.println("Disk pages written =" + res[3]);
        System.out.println("Unique labels in the file ="+ res[4]);

        if(counter == content.size())
            return true;
        else return false;
    }
}

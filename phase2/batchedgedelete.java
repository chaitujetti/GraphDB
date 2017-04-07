package phase2;


import diskmgr.*;
import global.Descriptor;
import global.EID;
import global.GlobalConst;
import global.SystemDefs;
import heap.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by joelmascarenhas on 3/11/17.
 */
public class batchedgedelete implements GlobalConst{
    public batchedgedelete() { }

    static boolean edgedelete(String filename, String dbname, SystemDefs systemdef)
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
            String[] temp = content.get(i).split(" ");
            String edgeLabel = temp[2];
            EScan edgescan = systemdef.JavabaseDB.getEhf().openScan();
            EID start_eid = new EID();
            EID deleid = new EID();
            Edge current_edge = new Edge();
            boolean deledge = false;
    
            while (!deledge) {
                current_edge = edgescan.getNext(start_eid);
                if(edgeLabel == "7_473"){
                    System.out.println(current_edge.getLabel());
                }
                // System.out.println(current_edge.getLabel());
                if (current_edge.getLabel().equals(edgeLabel)) {
                    deledge = true;
                    deleid.copyRid(start_eid);
                }
            }
            boolean stat = systemdef.JavabaseDB.deleteEdgeFromGraphDB(deleid);
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

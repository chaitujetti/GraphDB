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
        int counter = 0;
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
                if(current_edge==null){
                    break;
                }
                if (current_edge.getLabel().equals(edgeLabel)) {
                    deledge = true;
                    deleid.copyRid(start_eid);
                }
            }

            if(deledge){
                boolean stat = systemdef.JavabaseDB.deleteEdgeFromGraphDB(deleid);
            } else {
                System.out.println("No Existing Edge: " + temp[2]);
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
}

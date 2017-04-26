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
        int counter = 0;
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
                if(current_node == null){
                    break;
                }
                if (current_node.getLabel().equals(temp)) {
                    delnode = true;
                    delnid.copyRid(start_nid);
                }
            }
            
            if(delnode) {
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
            } else {
                System.out.println("No Existing Node: " + temp);
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

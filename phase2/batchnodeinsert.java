package phase2;

import diskmgr.*;
import bufmgr.*;
import global.Descriptor;
import global.GlobalConst;
import global.NID;
import global.SystemDefs;
import heap.*;
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
                // byte[] tempiter = iterator.getTupleByteArray();
                // Node nd=new Node(tempiter,0);
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
        System.out.println("Unique labels in the file =" + systemdef.JavabaseDB.getLabelCnt());

        if(counter == content.size())
            return true;
        else return false;
    }

    public static boolean checkNodeExists(String node, SystemDefs systemdef)
        throws IOException, InvalidTupleSizeException, InvalidTypeException, FieldNumberOutOfBoundException
    {
        NScan nodescan = systemdef.JavabaseDB.getNhf().openScan();
        NID start_nid = new NID();
        Node current_node;
        boolean existingNode = false;

        while (!existingNode) {
            current_node = nodescan.getNext(start_nid);
            if(current_node==null){
                break;
            }
            if (current_node.getLabel().equals(node)) {
                existingNode = true;
                nodescan.closescan();
            }
        }

        return existingNode;
    }
}

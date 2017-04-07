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
        
        int[] res = new int[]{0,0,0,0,0};
        int counter = 0;
        System.out.println(filename);
        boolean status = false;
        List<String> content = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filename))) {
            String lineread;
            while ((lineread = br.readLine()) != null) {
                content.add(lineread);
            }
        }

        Node iterator = new Node();
        NID nid = new NID();
        Descriptor desc = new Descriptor();

        for(int i=0;i<content.size();i++)
        {
            String[] temp = content.get(i).split(" ");
            desc.set(Short.valueOf(temp[1]),Short.valueOf(temp[2]),Short.valueOf(temp[3]),Short.valueOf(temp[4]),Short.valueOf(temp[5]));

            iterator.setLabel(temp[0]);
            iterator.setDesc(desc);
            byte[] tempiter = iterator.getTupleByteArray();
            //Node nd = new Node(tempiter,0,0);
            Node nd=new Node(tempiter,0);
            systemdef.JavabaseDB.insertNodeIntoGraphDB(nd.getTupleByteArray());

            counter++;
        }
        // get the node count
        res[0] = systemdef.JavabaseDB.getNodeCnt();

        // get the edge count
        res[1] = systemdef.JavabaseDB.getEdgeCnt();

        // get the pages read count
        res[2] = systemdef.JavabaseDB.getNoOfReads();

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

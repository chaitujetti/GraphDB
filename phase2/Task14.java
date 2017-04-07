package phase2;

import btree.*;
import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
// import ztree.ZTreeFile;
// import zindex.*;
// import ztree.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;

public class Task14 implements GlobalConst
{
    public Task14(){}

    static List<Edge> getEdgesFromHeapFile(GraphDB phase2) throws IOException,
            InvalidPageNumberException,
            FileIOException,
            DiskMgrException,
            FieldNumberOutOfBoundException,
            HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException, SpaceNotAvailableException, InvalidTupleSizeException,
            btree.GetFileEntryException, btree.ConstructPageException, btree.AddFileEntryException,ztree.GetFileEntryException,ztree.ConstructPageException,ztree.AddFileEntryException
    {
        EdgeHeapfile ehf=phase2.getEhf();
        EScan edgescan=ehf.openScan();
        EID eid=new EID();
        Edge currentEdge;
        List<Edge> edgeList=new ArrayList<>();
        while (true)
        {
            currentEdge=edgescan.getNext(eid);
            if(currentEdge==null)
            {
                break;
            }
            edgeList.add(currentEdge);
        }
        return edgeList;
    }

    static List<Node> getNodesFromHeapFile(GraphDB phase2) throws IOException,
            InvalidPageNumberException,
            FileIOException,
            DiskMgrException,
            FieldNumberOutOfBoundException,
            HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException, SpaceNotAvailableException, InvalidTupleSizeException,
            btree.GetFileEntryException, btree.ConstructPageException, btree.AddFileEntryException,ztree.GetFileEntryException,ztree.ConstructPageException,ztree.AddFileEntryException
    {
        NodeHeapfile nhf=phase2.getNhf();
        NScan nodescan=nhf.openScan();
        NID nid=new NID();
        Node currentNode;
        List<Node> nodeList=new ArrayList<>();
        while (true)
        {
            currentNode=nodescan.getNext(nid);
            if(currentNode==null)
            {
                break;
            }
            nodeList.add(currentNode);
        }
        return nodeList;
    }

    public static String getLabel(NID nid,GraphDB phase2) throws Exception
    {
        NodeHeapfile nhf=phase2.getNhf();
        Node node=nhf.getNode(nid);
        //System.out.println(node.getLabel());
        return node.getLabel();
    }

    public static void sortNodes(List<Node> nodesArray, GraphDB phase2) throws Exception
    {
        Node temp;
        for (int i = 0; i < nodesArray.size(); i++)
        {
            for (int j = i + 1; j < nodesArray.size(); j++)
            {
                if((nodesArray.get(i).getLabel()).compareTo(nodesArray.get(j).getLabel())>0)
                {
                    temp = nodesArray.get(i);
                    nodesArray.set(i,nodesArray.get(j));
                    nodesArray.set(j,temp);
                }
            }
        }
        for(int i = 0; i < nodesArray.size(); i++)
        {
            nodesArray.get(i).print();
            System.out.println();
        }
    }

    static void executeQueryTypeZero(int index, String dbname, SystemDefs systemdef) throws Exception
    {
//        GraphDB phase2 = new GraphDB(0);
//        phase2.openDB(dbname,1024);

        if(index==1)
        {
            System.out.println("Cannot execute this option without scanning the heap file");
            index=0;
        }
        List<Node> allNodes=getNodesFromHeapFile(systemdef.JavabaseDB);
        for(int i=0;i<allNodes.size();i++)
        {
            allNodes.get(i).print();
            System.out.println();
        }
    }

    public static void executeQueryTypeOne(int index,String dbname,SystemDefs systemdef) throws Exception
    {
//        GraphDB phase2 = new GraphDB(0);
//        phase2.openDB(dbname,1024);
        NodeHeapfile nhf=systemdef.JavabaseDB.getNhf();
        if(index==0)
        {
            List<Node> allNodes=getNodesFromHeapFile(systemdef.JavabaseDB);
            sortNodes(allNodes,systemdef.JavabaseDB);
        }
        if(index==1)
        {
            BTreeFile indexBTreeFile;

            indexBTreeFile=systemdef.JavabaseDB.nodeLabels_BFile;

            BTFileScan btscan=indexBTreeFile.new_scan(null,null);
            KeyDataEntry entry;
            while (true)
            {
                entry=btscan.get_next();
                if(entry==null)
                {
                    break;
                }
                try
                {
                    btree.LeafData leafNode=(btree.LeafData)entry.data;
                    RID rid=leafNode.getData();
                    NID nid=new NID();
                    nid.pageNo.pid=rid.pageNo.pid;
                    nid.slotNo=rid.slotNo;
                    Node node=nhf.getNode(nid);
                    node.print();
                    System.out.println();
                }
                catch(Exception e)
                {
                    System.out.println("Record at given slot has been deleted");
                }
            }
        }
    }

    public static void executeQueryTypeTwo(int index,String dbname,short values0,short values1,short values2,short values3,short values4,SystemDefs systemdef) throws Exception
    {
//        GraphDB phase2 = new GraphDB(0);
//        phase2.openDB(dbname,1024);
        short[] values=new short[5];
        Descriptor desc=new Descriptor();
        desc.set(values0,values1,values2,values3,values4);

        NodeHeapfile nhf=systemdef.JavabaseDB.getNhf();
        if(index==0)
        {
            List<Node> allNodes=getNodesFromHeapFile(systemdef.JavabaseDB);
            TreeMap<Integer,Node> treemap=new TreeMap<Integer,Node>();
            //sortNodes(allNodes,systemdef.JavabaseDB);
            for (int i=0;i<allNodes.size();i++)
            {
                Descriptor tempDesc=allNodes.get(i).getDesc();
                int distance=(int)tempDesc.distance(desc);
                treemap.put(distance,allNodes.get(i));
            }
            Set set=treemap.entrySet();
            Iterator iter=set.iterator();
            while(iter.hasNext())
            {
                Map.Entry n=(Map.Entry)iter.next();
                Node key=(Node)n.getValue();
                key.print();
            }
        }
//        if(index==1)
//        {
//            ZTreeFile indexZTreeFile;
//
//            indexZTreeFile=systemdef.JavabaseDB.nodesDescriptors_ZFile;
//
//            ZTFileScan btscan=indexZTreeFile.new_scan(null,null);
//            DescriptorKeyDataEntry entry;
//            while (true)
//            {
//                entry=btscan.get_next();
//                if(entry==null)
//                {
//                    break;
//                }
//                ztree.LeafData leafNode=(ztree.LeafData)entry.data;
//                RID rid=leafNode.getData();
//                NID nid=new NID();
//                nid.pageNo.pid=rid.pageNo.pid;
//                nid.slotNo=rid.slotNo;
//                Node node=nhf.getNode(nid);
//                node.print();
//                System.out.println();
//           }
//        }
    }

//    public static void executeQueryTypeTwo(int index,String dbname,SystemDefs systemdef) throws Exception
//    {
//        executeQueryTypeOneToFour(index,dbname,2);
//    }
//
//    public static void executeQueryTypeThree(int index,String dbname,SystemDefs systemdef) throws Exception
//    {
//        executeQueryTypeOneToFour(index,dbname,3);
//    }

    public static void executeQueryTypeFour(int index,String dbname, String label,SystemDefs systemdef) throws Exception
    {
//        GraphDB phase2 = new GraphDB(0);
//        phase2.openDB(dbname,1024);
        NodeHeapfile nhf=systemdef.JavabaseDB.getNhf();
        EdgeHeapfile ehf=systemdef.JavabaseDB.getEhf();
    
        if(index==0)
        {
            List<Edge> allEdges=getEdgesFromHeapFile(systemdef.JavabaseDB);
            List<Node> allNodes=getNodesFromHeapFile(systemdef.JavabaseDB);

            List<Edge> outgoingEdgeList=new ArrayList<Edge>();
            List<Edge> incomingEdgeList=new ArrayList<Edge>();

            System.out.println("Node details are as follows:");
            for(Node node : allNodes)
            {
                if((label.compareTo(node.getLabel()))==0)
                {
                    node.print();
                    break;
                }
            }


            for(Edge edge : allEdges)
            {
                if((label.compareTo(getLabel(edge.getSource(),systemdef.JavabaseDB)))==0) {
                    outgoingEdgeList.add(edge);
                }
            
                else if((label.compareTo(getLabel(edge.getDestination(),systemdef.JavabaseDB)))==0) {
                    incomingEdgeList.add(edge);
                }
            }

            if(outgoingEdgeList.size()>0)
                System.out.println("Outgoing edges details are as follows:");

            for(Edge edge : outgoingEdgeList)
            {
                edge.print();
                System.out.println();            

            }

            if(incomingEdgeList.size()>0)
                System.out.println("Incoming edges details are as follows:");

            for(Edge edge : incomingEdgeList)
            {
                edge.print();
                System.out.println();            
            }

            
        }    
        if(index==1)
        {
            BTreeFile indexBTreeFile=systemdef.JavabaseDB.nodeLabels_BFile;
            BTFileScan btscan=indexBTreeFile.new_scan(new StringKey(label),new StringKey(label));
            KeyDataEntry desiredNodeData;
            desiredNodeData=btscan.get_next();
            if(desiredNodeData==null)
            {
                System.out.println("No node with the given label is present");
                return;
            }
            try
            {
                btree.LeafData leafNode=(btree.LeafData)desiredNodeData.data;
                RID rid=leafNode.getData();
                NID nid=new NID();
                nid.pageNo.pid=rid.pageNo.pid;
                nid.slotNo=rid.slotNo;
                Node node=nhf.getNode(nid);
                node.print();
                System.out.println();
            }
            catch(Exception e)
            {
                System.out.println("Record at given slot has been deleted");
            }

            //Outgoing edges
            System.out.println("Outgoing edges");
            indexBTreeFile=systemdef.JavabaseDB.edgeSourceLabels_BFile;
            btscan=indexBTreeFile.new_scan(new StringKey(label),new StringKey(label));
            KeyDataEntry entry;
            while (true)
            {
                entry=btscan.get_next();
                if(entry==null)
                {
                    break;
                }
                try
                {
                    btree.LeafData leafNode=(btree.LeafData)entry.data;
                    //EID eid=(EID)leafNode.getData();
                    RID rid=leafNode.getData();
                    EID eid=new EID();
                    eid.pageNo.pid=rid.pageNo.pid;
                    eid.slotNo=rid.slotNo;
                    Edge edge=ehf.getEdge(eid);
                    edge.print();
                    System.out.println();
                }
                catch (Exception e)
                {
                    System.out.println("Record at given slot has been deleted");
                }
            }

            //Incoming edges
            System.out.println("Incoming edges");
            indexBTreeFile=systemdef.JavabaseDB.edgeDestinationLabels_BFile;
            btscan=indexBTreeFile.new_scan(new StringKey(label),new StringKey(label));
            while (true)
            {
                entry=btscan.get_next();
                if(entry==null)
                {
                    break;
                }
                try
                {
                    btree.LeafData leafNode=(btree.LeafData)entry.data;
                    //EID eid=(EID)leafNode.getData();
                    RID rid=leafNode.getData();
                    EID eid=new EID();
                    eid.pageNo.pid=rid.pageNo.pid;
                    eid.slotNo=rid.slotNo;
                    Edge edge=ehf.getEdge(eid);
                    edge.print();
                    System.out.println();
                }
                catch(Exception e)
                {
                    System.out.println("Record at given slot has been deleted");
                }
            }
        }

    }

    
}
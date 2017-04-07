package phase2;

import btree.*;
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

public class task15_0 implements GlobalConst
{
    public task15_0(){}

    static List<Edge> getEdgesFromHeapFile(GraphDB phase2) throws IOException,
            InvalidPageNumberException,
            FileIOException,
            DiskMgrException,
            FieldNumberOutOfBoundException,
            HFException, HFBufMgrException, HFDiskMgrException, InvalidSlotNumberException, SpaceNotAvailableException, InvalidTupleSizeException,
            GetFileEntryException, ConstructPageException, AddFileEntryException,ztree.GetFileEntryException,ztree.ConstructPageException,ztree.AddFileEntryException
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
            GetFileEntryException, ConstructPageException, AddFileEntryException,ztree.GetFileEntryException,ztree.ConstructPageException,ztree.AddFileEntryException
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
        return node.getLabel();
    }

    public static int compareUtil(Edge e1, Edge e2, int qtype,GraphDB phase2) throws Exception
    {
        switch(qtype)
        {
            case 1: return getLabel(e1.getSource(),phase2).compareTo(getLabel(e2.getSource(),phase2));
            case 2: return getLabel(e1.getDestination(),phase2).compareTo(getLabel(e2.getDestination(),phase2));
            case 3: return e1.getLabel().compareTo(e2.getLabel());
            case 4: return (new Integer(e1.getWeight())).compareTo(new Integer(e2.getWeight()));
            default: return 0;
        }
    }

    public static void sortEdges(List<Edge> edgesArray, int qtype,GraphDB phase2) throws Exception
    {
        Edge temp;
        for (int i = 0; i < edgesArray.size(); i++)
        {
            for (int j = i + 1; j < edgesArray.size(); j++)
            {
                if(compareUtil(edgesArray.get(i),edgesArray.get(j),qtype,phase2)>0)
                {
                    temp = edgesArray.get(i);
                    edgesArray.set(i,edgesArray.get(j));
                    edgesArray.set(j,temp);
                }
            }
        }
        for(int i = 0; i < edgesArray.size(); i++)
        {
            edgesArray.get(i).print();
            System.out.println();
        }
    }

    static void executeQueryTypeZero(int index,String dbname,SystemDefs systemdef) throws Exception
    {
//        GraphDB phase2 = new GraphDB(0);
//        phase2.openDB(dbname,1024);

        if(index==1)
        {
            System.out.println("Cannot execute this option without scanning the heap file");
            index=0;
        }
        List<Edge> allEdges=getEdgesFromHeapFile(systemdef.JavabaseDB);
        for(int i=0;i<allEdges.size();i++)
        {
            allEdges.get(i).print();
            System.out.println();
        }
    }

    public static void executeQueryTypeOne(int index, String dbname, SystemDefs systemdef) throws Exception
    {
        executeQueryTypeOneToFour(index,dbname,1,systemdef);
    }

    public static void executeQueryTypeTwo(int index,String dbname, SystemDefs systemdef) throws Exception
    {
        executeQueryTypeOneToFour(index,dbname,2,systemdef);
    }

    public static void executeQueryTypeThree(int index,String dbname, SystemDefs systemdef) throws Exception
    {
        executeQueryTypeOneToFour(index,dbname,3,systemdef);
    }

    public static void executeQueryTypeFour(int index,String dbname, SystemDefs systemdef) throws Exception
    {
        executeQueryTypeOneToFour(index,dbname,4,systemdef);
    }


    public static void executeQueryTypeOneToFour(int index,String dbname,int queryType,SystemDefs systemdef) throws Exception
    {
//        GraphDB phase2 = new GraphDB(0);
//        phase2.openDB(dbname,1024);

        EdgeHeapfile ehf=systemdef.JavabaseDB.getEhf();
        if(index==0)
        {
            List<Edge> allEdges=getEdgesFromHeapFile(systemdef.JavabaseDB);
            sortEdges(allEdges,queryType,systemdef.JavabaseDB);
        }
        if(index==1)
        {
            BTreeFile indexBTreeFile;

            if(queryType==1)
                indexBTreeFile=systemdef.JavabaseDB.edgeSourceLabels_BFile;
            else if(queryType==2)
                indexBTreeFile=systemdef.JavabaseDB.edgeDestinationLabels_BFile;
            else if(queryType==3)
                indexBTreeFile=systemdef.JavabaseDB.edgeLabels_BFile;
            else
                indexBTreeFile=systemdef.JavabaseDB.edgeWeights_BFile;

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
                    LeafData leafNode=(LeafData)entry.data;
                    RID rid=leafNode.getData();
                    EID eid=new EID();
                    eid.pageNo.pid=rid.pageNo.pid;
                    eid.slotNo=rid.slotNo;
                    Edge edge=ehf.getEdge(eid);
                    edge.print();
                    System.out.println();
                }
                catch(Exception e){
                    System.out.println("Record at given slot has been deleted");
                }
            }
        }
    }
    public static void executeQueryTypeFive(int index,String dbname,int lowerBound, int upperBound,SystemDefs systemdef) throws Exception {
//        GraphDB phase2 = new GraphDB(0);
//        phase2.openDB(dbname, 1024);
        EdgeHeapfile ehf = systemdef.JavabaseDB.getEhf();
        if (index == 0) {
            List<Edge> allEdges = getEdgesFromHeapFile(systemdef.JavabaseDB);
            List<Edge> filteredEdges = new ArrayList<Edge>();
            for (int i = 0; i < allEdges.size(); i++) {
                int val = allEdges.get(i).getWeight();
                if (val >= lowerBound && val <= upperBound) {
                    filteredEdges.add(allEdges.get(i));
                }
            }
            sortEdges(filteredEdges, 4, systemdef.JavabaseDB);
        }
        if (index == 1){
            BTreeFile indexBTreeFile=systemdef.JavabaseDB.edgeWeights_BFile;
            BTFileScan btscan=indexBTreeFile.new_scan(new IntegerKey(lowerBound),new IntegerKey(upperBound));
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
                    LeafData leafNode=(LeafData)entry.data;
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

    public static void executeQueryTypeSix(int index,String dbname,SystemDefs systemdef) throws Exception {

        

//        GraphDB phase2 = new GraphDB(0);
//        phase2.openDB(dbname, 1024);
        
        EdgeHeapfile ehf = systemdef.JavabaseDB.getEhf();
        

        if (index == 0) {
            List<Edge> allEdges=getEdgesFromHeapFile(systemdef.JavabaseDB);
            List<Node> allNodes=getNodesFromHeapFile(systemdef.JavabaseDB);

            List<Edge> outgoingEdgeList=new ArrayList<Edge>();
            List<Edge> incomingEdgeList=new ArrayList<Edge>();
            
            for(Node node : allNodes)
            {
                for(Edge edge: allEdges)
                {
                    //System.out.println("In loop of edges");
                    if(getLabel(edge.getSource(),systemdef.JavabaseDB).compareTo(node.getLabel())==0) {
//                        System.out.println("In loop of outgoing");
                        outgoingEdgeList.add(edge);
                    }
                    if (getLabel(edge.getDestination(),systemdef.JavabaseDB).compareTo(node.getLabel())==0){
//                        System.out.println("In loop of incoming");
                        incomingEdgeList.add(edge);
                    }
                }


                node.print();
                System.out.println();
                
                for(Edge edge1 : incomingEdgeList)
                {
                    System.out.println("Edge pair:");
                    for(Edge edge2 : outgoingEdgeList)
                    {
                        edge1.print();
                        System.out.println("AND");
                        edge2.print();
                    }
                }
                incomingEdgeList.clear();
                outgoingEdgeList.clear();
            }
        }
        if(index==1)
        {
            List<Node> allNodes=getNodesFromHeapFile(systemdef.JavabaseDB);
            BTreeFile btree_Source=systemdef.JavabaseDB.edgeSourceLabels_BFile;
            BTreeFile btree_Destn=systemdef.JavabaseDB.edgeDestinationLabels_BFile;
            for(int i=0;i<allNodes.size();i++)
            {
                Node node=allNodes.get(i);
                BTFileScan btscan=btree_Source.new_scan(new StringKey(node.getLabel()),new StringKey(node.getLabel()));
                node.print();
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
                        LeafData leafNode=(LeafData)entry.data;
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

                btscan=btree_Destn.new_scan(new StringKey(node.getLabel()),new StringKey(node.getLabel()));
                while (true)
                {
                    entry=btscan.get_next();
                    if(entry==null)
                    {
                        break;
                    }
                    try
                    {
                        LeafData leafNode=(LeafData)entry.data;
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

}
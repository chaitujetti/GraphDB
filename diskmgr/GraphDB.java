package diskmgr;

import java.io.*;
import java.util.*;

import btree.*;
import btree.ConstructPageException;
import btree.ConvertException;
import btree.DeleteFashionException;
import btree.DeleteRecException;
import btree.DescriptorKey;
import btree.FreePageException;
import btree.GetFileEntryException;
import btree.IndexFullDeleteException;
import btree.IndexInsertRecException;
import btree.IndexSearchException;
import btree.InsertException;
import btree.InsertRecException;
import btree.IteratorException;
import btree.KeyNotMatchException;
import btree.KeyTooLongException;
import btree.LeafDeleteException;
import btree.LeafInsertRecException;
import btree.LeafRedistributeException;
import btree.NodeNotMatchException;
import btree.PinPageException;
import btree.RecordNotFoundException;
import btree.RedistributeException;
import btree.UnpinPageException;
import bufmgr.*;
import global.*;
import heap.*;
import ztree.*;

/**
 * Created by vamsikrishnag on 3/12/17.
 */


public class GraphDB extends DB
{
    public String DBname;

    public NodeHeapfile nhf;
    public EdgeHeapfile ehf;

    public BTreeFile nodeLabels_BFile;
    public BTreeFile edgeLabels_BFile;
    public BTreeFile edgeSourceLabels_BFile;
    public BTreeFile edgeDestinationLabels_BFile;
    public BTreeFile edgeWeights_BFile;

    public ZTreeFile nodesDescriptors_ZFile;

    public static int Graphcounter;
    public int GraphID;
    public int type;

    public HashMap<String,Integer> hashLabelsPresent;
    public HashMap<String,Integer> hashSourceNodesPresent;
    public HashMap<String,Integer> hashDestinationNodesPresent;



    public GraphDB(int type) throws InvalidSlotNumberException,InvalidTupleSizeException,HFException,
            HFBufMgrException,HFDiskMgrException,GetFileEntryException,ConstructPageException,IOException, ztree.AddFileEntryException,
            btree.AddFileEntryException,ztree.GetFileEntryException,ztree.ConstructPageException
    {
        super();
        this.type = type;
        GraphID = Graphcounter;
        Graphcounter++;
        DBname = "DB_"+String.valueOf(GraphID);

        hashSourceNodesPresent = new HashMap<>();
        hashDestinationNodesPresent = new HashMap<>();
        hashLabelsPresent = new HashMap<>();

        PCounter.initialize();

    }

    public void createIndexFiles(String DBname) throws InvalidSlotNumberException,InvalidTupleSizeException,HFException,
            HFBufMgrException,HFDiskMgrException,GetFileEntryException,ConstructPageException,IOException, ztree.AddFileEntryException,
            btree.AddFileEntryException,ztree.GetFileEntryException,ztree.ConstructPageException
    {
        nhf=new NodeHeapfile("NodeHeapFile_"+DBname);
        ehf=new EdgeHeapfile("EdgeHeapFile_"+DBname);
        nodeLabels_BFile=new BTreeFile("NodeLabelsBtree_"+DBname,AttrType.attrString,100,1);
        edgeLabels_BFile=new BTreeFile("EdgeLabelsBtree_"+DBname,AttrType.attrString,100,1);
        edgeSourceLabels_BFile=new BTreeFile("EdgeSourceLabelsBtree_"+DBname,AttrType.attrString,100,1);
        edgeDestinationLabels_BFile=new BTreeFile("EdgeDestinationLabelsBtree_"+DBname,AttrType.attrString,100,1);
        edgeWeights_BFile=new BTreeFile("EdgeWeightsBtree_"+DBname,AttrType.attrInteger,4,1);

        nodesDescriptors_ZFile=new ZTreeFile("NodeDescriptorsZtree_"+DBname,AttrType.attrDesc,10,1);

    }

    public int getNoOfReads()
    {
        return PCounter.rcounter;
    }
    public int getNoOfWrites()
    {
        return PCounter.wcounter;
    }

    public int getNodeCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFBufMgrException, HFDiskMgrException, IOException {
        return nhf.getNodeCnt();
    }

    public int getEdgeCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFBufMgrException, HFDiskMgrException, IOException {
        return ehf.getEdgeCnt();
    }

    public void updateNodeLabels(String label,int type) //hashLabelsPresent
    {
        if(type==0)//0 is insert
        {
            if(hashLabelsPresent.containsKey(label))
            {
                int val=hashLabelsPresent.get(label);
                hashLabelsPresent.put(label,val+1);
            }
            else {
                hashLabelsPresent.put(label,1);
            }
        }
        else //delete
        {
            if(hashLabelsPresent.containsKey(label))
            {
                int val=hashLabelsPresent.get(label);
                if(val==1)
                {
                    hashLabelsPresent.remove(label);
                }
                else
                {
                    hashLabelsPresent.put(label,val-1);
                }
            }
        }
    }

    public int getLabelCnt() throws InvalidSlotNumberException,InvalidTupleSizeException,HFException,HFBufMgrException,HFDiskMgrException,Exception
    {
        return hashLabelsPresent.size();
    }

    public void updateEdgeNodeLabels(NID nid,HashMap<String,Integer> hashTable,int type)
    {
        int Pid=nid.pageNo.pid;
        int slotid=nid.slotNo;
        String nid_str=Integer.toString(Pid)+"_"+Integer.toString(slotid);
        if(type==0)//0 is insert
        {
            if (hashTable.containsKey(nid_str))
            {
                int val=hashTable.get(nid_str);
                hashTable.put(nid_str,val+1);
            }
            else{
                hashTable.put(nid_str,1);
            }
        }
        else //1 is delete
        {
            if (hashTable.containsKey(nid_str))
            {
                int val=hashTable.get(nid_str);
                if(val==1){
                    hashTable.remove(nid_str);
                }
                else {
                    hashTable.put(nid_str,val-1);
                }
            }
        }
    }

    public int getSourceCnt() throws
            HFException,HFDiskMgrException,HFBufMgrException,IOException,InvalidSlotNumberException,InvalidTupleSizeException,FieldNumberOutOfBoundException
    {
        return hashSourceNodesPresent.size();
    }


    public int getDestinationCnt() throws
            HFException,HFDiskMgrException,HFBufMgrException,IOException,InvalidSlotNumberException,InvalidTupleSizeException,FieldNumberOutOfBoundException
    {
        return hashDestinationNodesPresent.size();
    }

    public void insertNodeIntoGraphDB(byte[] nodeByteArray) throws Exception
    {
        NID nid=nhf.insertNode(nodeByteArray);
        //System.out.println("NID data:"+nid.pageNo.pid+" "+nid.slotNo);
        Node node=nhf.getNode(nid);
        insertNodeIntoIndex(nid,node);
        updateNodeLabels(node.getLabel(),0);//insert node
    }

    public void insertEdgeIntoGraphDB(byte[] edgeByteArray) throws Exception
    {
        EID eid=ehf.insertEdge(edgeByteArray);
        //System.out.println("NID data:"+nid.pageNo.pid+" "+nid.slotNo);
        Edge edge=ehf.getEdge(eid);
        NID sourceNID=edge.getSource();
        NID destinationNID=edge.getDestination();
        Node source=nhf.getNode(sourceNID);
        Node destination=nhf.getNode(destinationNID);
        insertEdgeIntoIndex(eid,edge,source,destination);
        //updateNodeLabels(edge.getLabel(),0);//insert node
        updateEdgeNodeLabels(sourceNID,hashSourceNodesPresent,0);
        updateEdgeNodeLabels(destinationNID,hashDestinationNodesPresent,0);
    }

    public boolean deleteNodeFromGraphDB(NID nid) throws Exception
    {
        try {
            Node node = nhf.getNode(nid);
            deleteNodeFromIndex(nid, node);
            updateNodeLabels(node.getLabel(), 1);
            nhf.deleteNode(nid);
        }
        catch (Exception e){
            return false;
        }
        return false;
    }

    public boolean deleteEdgeFromGraphDB(EID eid) throws  Exception
    {
        try {
            Edge edge = ehf.getEdge(eid);
            NID sourceNID = edge.getSource();
            NID destinationNID = edge.getDestination();
            Node source = nhf.getNode(sourceNID);
            Node destination = nhf.getNode(destinationNID);
            deleteEdgeFromIndex(eid, edge, source, destination);
            updateEdgeNodeLabels(sourceNID, hashSourceNodesPresent, 1);
            updateEdgeNodeLabels(destinationNID, hashDestinationNodesPresent, 1);
            ehf.deleteEdge(eid);
            return true;
        }
        catch (Exception e){
            return false;
        }
    }

    public void insertEdgeIntoIndex(EID eid,Edge edge,Node source,Node destination) throws KeyTooLongException,KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException,
            PinPageException, NodeNotMatchException, ConvertException,DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException,
            InsertException,IOException,FieldNumberOutOfBoundException
    {
        String label=edge.getLabel();
        edgeLabels_BFile.insert(new StringKey(label),eid);
        String sourceLabel=source.getLabel();
        edgeSourceLabels_BFile.insert(new StringKey(sourceLabel),eid/*edge.getSource()*/);
        String destinationLabel=destination.getLabel();
        edgeDestinationLabels_BFile.insert(new StringKey(destinationLabel),eid/*edge.getDestination()*/);
        int weights=edge.getWeight();
        edgeWeights_BFile.insert(new IntegerKey(weights),eid);
        updateEdgeNodeLabels(edge.getSource(),hashSourceNodesPresent,0);//Insert Source Node
        updateEdgeNodeLabels(edge.getDestination(),hashDestinationNodesPresent,0);//Insert Destination Node
    }

    public void deleteEdgeFromIndex(EID eid,Edge edge,Node source,Node destination) throws IOException, FieldNumberOutOfBoundException, DeleteFashionException,
            LeafRedistributeException, RedistributeException, InsertRecException, KeyNotMatchException, UnpinPageException, IndexInsertRecException,
            FreePageException, RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException, IteratorException,
            ConstructPageException, DeleteRecException, IndexSearchException
    {
        String label=edge.getLabel();
        edgeLabels_BFile.Delete(new StringKey(label),eid);
        String sourceLabel=source.getLabel();
        edgeSourceLabels_BFile.Delete(new StringKey(sourceLabel),edge.getSource());
        String destinationLabel=destination.getLabel();
        edgeDestinationLabels_BFile.Delete(new StringKey(destinationLabel),edge.getDestination());
        int weights=edge.getWeight();
        edgeWeights_BFile.Delete(new IntegerKey(weights),eid);
        updateEdgeNodeLabels(edge.getSource(),hashSourceNodesPresent,1);//Delete Source Node
        updateEdgeNodeLabels(edge.getDestination(),hashDestinationNodesPresent,1);//Delete Destination Node
    }

    public void insertNodeIntoIndex(NID nid,Node node) throws FieldNumberOutOfBoundException, KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException,
            PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
            LeafDeleteException, InsertException, IOException, ztree.KeyTooLongException, ztree.KeyNotMatchException, ztree.LeafInsertRecException, ztree.IndexInsertRecException, ztree.ConstructPageException,
            ztree.UnpinPageException, ztree.PinPageException, ztree.NodeNotMatchException, ztree.ConvertException, ztree.DeleteRecException, ztree.IndexSearchException, ztree.IteratorException,
            ztree.LeafDeleteException, ztree.InsertException
    {
        String label=node.getLabel();
        nodeLabels_BFile.insert(new StringKey(label),nid);
        Descriptor desc=node.getDesc();
        nodesDescriptors_ZFile.insert(new ztree.DescriptorKey(desc),nid);
    }

    public void deleteNodeFromIndex(NID nid,Node node) throws FieldNumberOutOfBoundException, DeleteFashionException, LeafRedistributeException,
            RedistributeException,InsertRecException,KeyNotMatchException, UnpinPageException,IndexInsertRecException,FreePageException,
            RecordNotFoundException, PinPageException,IndexFullDeleteException,LeafDeleteException,IteratorException,ConstructPageException, DeleteRecException,
            IndexSearchException, IOException, ztree.DeleteFashionException, ztree.LeafRedistributeException,ztree.RedistributeException, ztree.InsertRecException, ztree.KeyNotMatchException, ztree.UnpinPageException,
            ztree.IndexInsertRecException, ztree.FreePageException, ztree.RecordNotFoundException, ztree.PinPageException, ztree.IndexFullDeleteException, ztree.LeafDeleteException, ztree.IteratorException, ztree.ConstructPageException,
            ztree.DeleteRecException, ztree.IndexSearchException
    {
        String label=node.getLabel();
        nodeLabels_BFile.Delete(new StringKey(label),nid);
        Descriptor desc=node.getDesc();
        nodesDescriptors_ZFile.Delete(new ztree.DescriptorKey(desc),nid);
        updateNodeLabels(label,1);//delete node
    }

    public NodeHeapfile getNhf () {
        return nhf;
    }

    public EdgeHeapfile getEhf () {
        return ehf;
    }

}

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
    
    public static int Graphcounter;
    public int GraphID;
    public int type;


    public GraphDB(int type) throws InvalidSlotNumberException,InvalidTupleSizeException,HFException,
            HFBufMgrException,HFDiskMgrException,GetFileEntryException,ConstructPageException,IOException, ztree.AddFileEntryException,
            btree.AddFileEntryException,ztree.GetFileEntryException,ztree.ConstructPageException
    {
        super();
        this.type = type;
        GraphID = Graphcounter;
        Graphcounter++;
        DBname = "DB_"+String.valueOf(GraphID);

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

        edgeWeights_BFile=new BTreeFile("EdgeWeights_BFile_"+DBname,AttrType.attrInteger,100,0);
    }

    public void insertNodeIntoGraphDB(byte[] nodeByteArray) throws Exception
    {
        NID nid=nhf.insertNode(nodeByteArray);
        Node node=nhf.getNode(nid);
        insertNodeIntoIndex(nid,node);
    }

    public void insertEdgeIntoGraphDB(byte[] edgeByteArray) throws Exception
    {
        EID eid=ehf.insertEdge(edgeByteArray);
        Edge edge=ehf.getEdge(eid);
        NID sourceNID=edge.getSource();
        NID destinationNID=edge.getDestination();
        Node source=nhf.getNode(sourceNID);
        Node destination=nhf.getNode(destinationNID);
        insertEdgeIntoIndex(eid,edge,source,destination);
    }

    public boolean deleteNodeFromGraphDB(NID nid) throws Exception
    {
        try {
            Node node = nhf.getNode(nid);
            deleteNodeFromIndex(nid, node);
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
            ehf.deleteEdge(eid);
            
            return true;
        }
        catch (Exception e){
            return false;
        }
    }

    public void insertEdgeIntoIndex(EID eid,Edge edge,Node source,Node destination) throws KeyTooLongException,KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException,
            PinPageException, NodeNotMatchException, ConvertException,DeleteRecException, IndexSearchException, IteratorException, LeafDeleteException,
            InsertException,IOException,FieldNumberOutOfBoundException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException
    {
        String label=edge.getLabel();
        edgeLabels_BFile.insert(new StringKey(label),eid);
        String sourceLabel=source.getLabel();
        edgeSourceLabels_BFile.insert(new StringKey(sourceLabel),eid/*edge.getSource()*/);
        String destinationLabel=destination.getLabel();
        edgeDestinationLabels_BFile.insert(new StringKey(destinationLabel),eid/*edge.getDestination()*/);
        int weights=edge.getWeight();
        edgeWeights_BFile.insert(new IntegerKey(weights), eid);
    }

    public void deleteEdgeFromIndex(EID eid,Edge edge,Node source,Node destination) throws IOException, FieldNumberOutOfBoundException, DeleteFashionException,
            LeafRedistributeException, RedistributeException, InsertRecException, KeyNotMatchException, UnpinPageException, IndexInsertRecException,
            FreePageException, RecordNotFoundException, PinPageException, IndexFullDeleteException, LeafDeleteException, IteratorException,
            ConstructPageException, DeleteRecException, IndexSearchException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException
    {
        String label=edge.getLabel();
        edgeLabels_BFile.Delete(new StringKey(label),eid);
        String sourceLabel=source.getLabel();
        edgeSourceLabels_BFile.Delete(new StringKey(sourceLabel),edge.getSource());
        String destinationLabel=destination.getLabel();
        edgeDestinationLabels_BFile.Delete(new StringKey(destinationLabel),edge.getDestination());
        int weights=edge.getWeight();
        edgeWeights_BFile.Delete(new IntegerKey(weights), eid);
    }

    public void insertNodeIntoIndex(NID nid,Node node) throws FieldNumberOutOfBoundException, KeyTooLongException, KeyNotMatchException, LeafInsertRecException, IndexInsertRecException, ConstructPageException, UnpinPageException,
            PinPageException, NodeNotMatchException, ConvertException, DeleteRecException, IndexSearchException, IteratorException,
            LeafDeleteException, InsertException, IOException, ztree.KeyTooLongException, ztree.KeyNotMatchException, ztree.LeafInsertRecException, ztree.IndexInsertRecException, ztree.ConstructPageException,
            ztree.UnpinPageException, ztree.PinPageException, ztree.NodeNotMatchException, ztree.ConvertException, ztree.DeleteRecException, ztree.IndexSearchException, ztree.IteratorException,
            ztree.LeafDeleteException, ztree.InsertException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException
    {
        String label=node.getLabel();
        nodeLabels_BFile.insert(new StringKey(label),nid);
        Descriptor desc=node.getDesc();
    }

    public void deleteNodeFromIndex(NID nid,Node node) throws FieldNumberOutOfBoundException, DeleteFashionException, LeafRedistributeException,
            RedistributeException,InsertRecException,KeyNotMatchException, UnpinPageException,IndexInsertRecException,FreePageException,
            RecordNotFoundException, PinPageException,IndexFullDeleteException,LeafDeleteException,IteratorException,ConstructPageException, DeleteRecException,
            IndexSearchException, IOException, ztree.DeleteFashionException, ztree.LeafRedistributeException,ztree.RedistributeException, ztree.InsertRecException, ztree.KeyNotMatchException, ztree.UnpinPageException,
            ztree.IndexInsertRecException, ztree.FreePageException, ztree.RecordNotFoundException, ztree.PinPageException, ztree.IndexFullDeleteException, ztree.LeafDeleteException, ztree.IteratorException, ztree.ConstructPageException,
            ztree.DeleteRecException, ztree.IndexSearchException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException
    {
        String label=node.getLabel();
        nodeLabels_BFile.Delete(new StringKey(label),nid);
        Descriptor desc=node.getDesc();
    }

    public NodeHeapfile getNhf () {
        return nhf;
    }

    public EdgeHeapfile getEhf () {
        return ehf;
    }

    public BTreeFile getNodeIndex() {
        return nodeLabels_BFile;
    }

    public BTreeFile getEdgeSourceIndex() {
        return edgeSourceLabels_BFile;
    } 

    public BTreeFile getEdgeDestinationIndex() {
        return edgeDestinationLabels_BFile;
    } 

    public BTreeFile getEdgeIndex() {
        return edgeLabels_BFile;
    } 

    public int getNoOfReads()
    {
        return PCounter.rcounter;
    }
    
    public int getNoOfWrites()
    {
        return PCounter.wcounter;
    }

    public void flushCounters(){
        PCounter.flushCounters();
    }

    public int getNodeCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFBufMgrException, HFDiskMgrException, IOException {
        return nhf.getNodeCnt();
    }

    public int getEdgeCnt() throws InvalidSlotNumberException, InvalidTupleSizeException, HFException, HFBufMgrException, HFDiskMgrException, IOException {
        return ehf.getEdgeCnt();
    }

}

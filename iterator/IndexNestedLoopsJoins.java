package iterator;

import btree.*;
import btree.ConstructPageException;
import btree.GetFileEntryException;
import btree.PinPageException;
import heap.*;
import global.*;
import bufmgr.*;
import diskmgr.*;
import index.*;

import java.lang.*;
import java.io.*;
import java.util.*;

public class IndexNestedLoopsJoins extends Iterator
{

    private CondExpr outerCondn[];
    private CondExpr innerCondn[];

    private NodeHeapfile outerNodeHF;
    private EdgeHeapfile outerEdgeHF;
    private NodeHeapfile innerNodeHF;
    private EdgeHeapfile innerEdgeHF;

    private AttrType[] edgeAttrs,nodeAttrs;

    private BTreeFile innerBTreeFile;

    private Scan outerRelScan;
    private BTFileScan innerRelScan;

    private Node outerNode;
    private Node innerNode;
    private Edge outerEdge;
    private Edge innerEdge;
    private KeyDataEntry entry;

    private RID outerRID;

    private int joinType;


    public IndexNestedLoopsJoins(int joinType, String outerRelHeapFile, String innerRelHeapFile, String innerRelIndexFile, CondExpr outerCondn[], CondExpr innerCondn[]) throws IOException, HFException, HFBufMgrException, HFDiskMgrException, ConstructPageException, GetFileEntryException, PinPageException, InvalidTupleSizeException {
        this.joinType= joinType;
        this.outerCondn=outerCondn;
        this.innerCondn=innerCondn;
        outerRID = new RID();
        nodeAttrs= new AttrType[2];
        nodeAttrs[0]= new AttrType(AttrType.attrString);
        nodeAttrs[1]= new AttrType(AttrType.attrDesc);

        edgeAttrs= new AttrType[8];
        edgeAttrs[0]= new AttrType(AttrType.attrString);
        for(int j=1;j<6;j++)
        {
            edgeAttrs[j]=new AttrType(AttrType.attrInteger);
        }
        edgeAttrs[6]= new AttrType(AttrType.attrString);
        edgeAttrs[7]= new AttrType(AttrType.attrString);

        innerRelScan = null;
        entry=null;

        if(joinType==1||joinType==2) {
            outerNodeHF = new NodeHeapfile(outerRelHeapFile);
            innerEdgeHF = new EdgeHeapfile(innerRelHeapFile);
            innerBTreeFile = new BTreeFile(innerRelIndexFile);

            outerRelScan = outerNodeHF.openScan();
        }
        else if(joinType==3||joinType==4)
        {
            outerEdgeHF = new EdgeHeapfile(outerRelHeapFile);
            innerNodeHF = new NodeHeapfile(innerRelHeapFile);
            innerBTreeFile = new BTreeFile(innerRelIndexFile);

            outerRelScan = outerEdgeHF.openScan();
        }
    }

    public Tuple get_next() throws Exception {
        if(joinType==1||joinType==2)
        {
            if(innerRelScan==null || entry==null ||(entry=innerRelScan.get_next())==null)
            {
                innerRelScan= null; //Just to make sure there is no Scan object if returned
                outerNode=(Node)outerRelScan.getNext(outerRID);
                if(outerNode==null) {
                    return null;
                }
                innerRelScan=innerBTreeFile.new_scan(new StringKey(outerNode.getLabel()),new StringKey(outerNode.getLabel()));
                entry= innerRelScan.get_next();
            }
            if(entry!=null) {
                LeafData leafNode = (LeafData) entry.data;
                RID rid = leafNode.getData();
                EID eid = new EID();
                eid.pageNo.pid = rid.pageNo.pid;
                eid.slotNo = rid.slotNo;
                innerEdge = innerEdgeHF.getEdge(eid);

                short[] strSizes = {10, 10, 10};    //Reference from global constant
                innerEdge.setHdr((short) 8, edgeAttrs, strSizes);  //8 change in Edge.java
                if (PredEval.Eval(innerCondn, innerEdge, null, edgeAttrs, null)) {
                    if (PredEval.Eval(outerCondn, outerNode, innerEdge, nodeAttrs, edgeAttrs)) {
                        return innerEdge;
                    }
                }
            }
        }
        else if(joinType==3||joinType==4)
        {
            if(innerRelScan==null || entry==null|| (entry=innerRelScan.get_next())==null)
            {
                innerRelScan=null;
                outerEdge=(Edge)outerRelScan.getNext(outerRID);
                if(outerEdge==null) {
                    return null;
                }
                if(joinType==3) {
                    innerRelScan = innerBTreeFile.new_scan(new StringKey(outerEdge.getSourceLabel()), new StringKey(outerEdge.getSourceLabel()));
                }
                if(joinType==4) {
                    innerRelScan = innerBTreeFile.new_scan(new StringKey(outerEdge.getDestinationLabel()), new StringKey(outerEdge.getDestinationLabel()));
                }
                entry= innerRelScan.get_next();
            }
            if(entry!=null) {
                LeafData leafNode = (LeafData) entry.data;
                RID rid = leafNode.getData();
                NID nid = new NID();
                nid.pageNo.pid = rid.pageNo.pid;
                nid.slotNo = rid.slotNo;
                innerNode = innerNodeHF.getNode(nid);

                short[] strSizes = {10};    //Reference from global constant
                innerNode.setHdr((short) 2, nodeAttrs, strSizes);
                if (PredEval.Eval(innerCondn, innerNode, null, nodeAttrs, null)) {
                    if (PredEval.Eval(outerCondn, outerEdge, innerNode, edgeAttrs, nodeAttrs)) {
                        return innerNode;
                    }
                }
            }
        }
        return null;
    }

    public void close() throws JoinsException, IOException,IndexException
    {
        if (!closeFlag)
        {
            try
            {
                outerRelScan.closescan();
            }
            catch (Exception e)
            {
                throw new JoinsException(e, "IndexNestedLoopsJoins.java: error in closing Outer Relation scan.");
            }
        }
    }
}
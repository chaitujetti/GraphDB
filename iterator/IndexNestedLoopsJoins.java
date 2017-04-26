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

    //private Scan outerRelScan;
    private BTFileScan innerRelScan;

    private Node outerNode;
    private Node innerNode;
    private Edge outerEdge;
    private Edge innerEdge;
    private KeyDataEntry entry;

    private RID outerRID;

    private int joinType;

    private short  stringSize;


    public IndexNestedLoopsJoins(int joinType, Heapfile outerRelHeapFile, Heapfile innerRelHeapFile, BTreeFile innerRelIndexFile, CondExpr outerCondn[], CondExpr innerCondn[], RID outerRID) throws Exception, IOException, HFException, HFBufMgrException, HFDiskMgrException, ConstructPageException, GetFileEntryException, PinPageException, InvalidTupleSizeException {

        stringSize = 10;

        this.joinType= joinType;
        this.outerCondn=outerCondn;
        this.innerCondn=innerCondn;
        this.outerRID = outerRID;
        nodeAttrs= new AttrType[2];
        nodeAttrs[0]= new AttrType(AttrType.attrString);
        nodeAttrs[1]= new AttrType(AttrType.attrDesc);

        edgeAttrs= new AttrType[8];
        edgeAttrs[0]= new AttrType(AttrType.attrString);
        edgeAttrs[1]= new AttrType(AttrType.attrString);
        edgeAttrs[2]= new AttrType(AttrType.attrString);
        for(int j=3;j<8;j++)
        {
            edgeAttrs[j]=new AttrType(AttrType.attrInteger);
        }


        innerRelScan = null;
        entry=null;

        if(joinType==1||joinType==2) {
            outerNodeHF = (NodeHeapfile) outerRelHeapFile; //new NodeHeapfile(outerRelHeapFile);   //Might have to change it
            innerEdgeHF = (EdgeHeapfile) innerRelHeapFile; //new EdgeHeapfile(innerRelHeapFile);   // so as to directly get the heap/index file object
            innerBTreeFile = innerRelIndexFile;  // instead of the filename

            //outerRelScan = outerNodeHF.openScan();
            NID tempNID = new NID();
            tempNID.pageNo.pid = outerRID.pageNo.pid;
            tempNID.slotNo = outerRID.slotNo;
            outerNode = outerNodeHF.getNode(tempNID);
            innerRelScan = innerBTreeFile.new_scan(new StringKey(outerNode.getLabel()), new StringKey(outerNode.getLabel()));

        }
        else if(joinType==3||joinType==4)
        {
            outerEdgeHF = (EdgeHeapfile)outerRelHeapFile;
            innerNodeHF = (NodeHeapfile)innerRelHeapFile;
            innerBTreeFile = innerRelIndexFile;

            //outerRelScan = outerEdgeHF.openScan();
            EID tempEID = new EID();
            //System.out.println("Outer RID:"+Integer.toString(outerRID.pageNo.pid)+","+Integer.toString(outerRID.slotNo));
            tempEID.pageNo.pid = outerRID.pageNo.pid;
            tempEID.slotNo = outerRID.slotNo;
            outerEdge = outerEdgeHF.getEdge(tempEID);
            if (joinType == 3) {
                innerRelScan = innerBTreeFile.new_scan(new StringKey(outerEdge.getSourceLabel()), new StringKey(outerEdge.getSourceLabel()));
            }
            if (joinType == 4) {
                innerRelScan = innerBTreeFile.new_scan(new StringKey(outerEdge.getDestinationLabel()), new StringKey(outerEdge.getDestinationLabel()));
            }
        }
    }

    public Tuple get_next() throws Exception
    {
        return null;
    }

    public Tuple get_next(RID rid) throws Exception {
        while((entry = innerRelScan.get_next()) != null) {
            if (joinType == 1 || joinType == 2) {

                if (outerNode == null) {
                    return null;
                }
                LeafData leafNode = (LeafData) entry.data;
                RID temprid = leafNode.getData();
                rid.pageNo.pid = temprid.pageNo.pid;
                rid.slotNo = temprid.slotNo;
                EID eid = new EID();
                eid.pageNo.pid = rid.pageNo.pid;
                eid.slotNo = rid.slotNo;
                innerEdge = innerEdgeHF.getEdge(eid);
                short[] strSizes = {stringSize, stringSize, stringSize};    //Reference from global constant
                innerEdge.setHdr((short) 8, edgeAttrs, strSizes);  //8 change in Edge.java
                if (PredEval.Eval(innerCondn, null, innerEdge, null, edgeAttrs)) {
                    if (PredEval.Eval(outerCondn, outerNode, innerEdge, nodeAttrs, edgeAttrs)) {
//                        System.out.println("Edge Label INLJ:"+innerEdge.getLabel());
                        //System.out.println("RID Passed:"+Integer.toString(temprid.pageNo.pid)+","+Integer.toString(temprid.slotNo));
                        //System.out.println("Edge Weight:"+Integer.toString(innerEdge.getWeight()));
                        return innerEdge;
                    }
                }

            } else if (joinType == 3 || joinType == 4) {

                if (outerEdge == null) {
                    return null;
                }
                LeafData leafNode = (LeafData) entry.data;
                RID temprid = leafNode.getData();
                rid.pageNo.pid = temprid.pageNo.pid;
                rid.slotNo = temprid.slotNo;

                NID nid = new NID();
                nid.pageNo.pid = rid.pageNo.pid;
                nid.slotNo = rid.slotNo;
                innerNode = innerNodeHF.getNode(nid);

                short[] strSizes = {stringSize};    //Reference from global constant
                innerNode.setHdr((short) 2, nodeAttrs, strSizes);
                if (PredEval.Eval(innerCondn, null, innerNode, null, nodeAttrs)) {
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
                //outerRelScan.closescan();
            }
            catch (Exception e)
            {
                throw new JoinsException(e, "IndexNestedLoopsJoins.java: error in closing Outer Relation scan.");
            }
        }
    }
}
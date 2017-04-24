package operator;

import btree.BTreeFile;
import heap.*;
import iterator.*;
import global.*;

import java.io.IOException;
import java.time.Clock;

/**
 * Created by ankur on 4/22/17.
 */
public class PathExpressionOperator3
{
    private RID root;
    private String rootLabel="";
    private int flag;
//    private int maxNumEdges;
//    private int maxTotalEdgeWeight;
    private int maxBound;
    private int condition;

    private NodeHeapfile nodeHeapFile;
    private EdgeHeapfile edgeHeapFile;
    private BTreeFile nodeIndexFile;
    private BTreeFile edgeSourceLabelIndexFile;

    private FileScan outputFilescan;

    Heapfile outputFile;

    private int position;

    public PathExpressionOperator3(int condition, RID firstNode, NodeHeapfile nodeHeapFile, EdgeHeapfile edgeHeapFile, BTreeFile nodeIndexFile, BTreeFile edgeSourceLabelIndexFile, String outputHeapFileName) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        root = new RID();
        root.pageNo.pid = firstNode.pageNo.pid;
        root.slotNo = firstNode.slotNo;
        this.nodeHeapFile = nodeHeapFile;
        this.edgeHeapFile = edgeHeapFile;
        this.nodeIndexFile = nodeIndexFile;
        this.edgeSourceLabelIndexFile = edgeSourceLabelIndexFile;
        flag=1;
        position =0;
        this.condition=condition;
        outputFile = new Heapfile(outputHeapFileName);
    }

    public FileScan getOutputFileScanObject()
    {
        FldSpec [] Sprojection = new FldSpec[1];
        Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);

        AttrType[] types= new AttrType[1];
        types[0] = new AttrType(AttrType.attrString);

        short [] Ssizes = new short [1];
        Ssizes[0] = 10;

        //FileScan am = null;
        try {
            outputFilescan  = new FileScan(outputFile._fileName, types, Ssizes,
                    (short)1, (short)1,
                    Sprojection, null);
        }
        catch (Exception e) {
            System.err.println (""+e);
        }
        return outputFilescan;
    }

    public void findTailNodes(int value) throws Exception {
        NID tempNID = new NID();
        tempNID.pageNo.pid = root.pageNo.pid;
        tempNID.slotNo = root.slotNo;
        Node rootNode = nodeHeapFile.getNode(tempNID);
        rootLabel=rootNode.getLabel();

        maxBound = value;
        DFS(root,1,0,condition);
    }

    public void DFS(RID rid, int flag, int weight, int boundType) throws Exception
    {
        if(flag==1) //Node Edge join
        {
            System.out.println("QP: IndexNestedLoopJoin Node|X|Edge Join Condition is Node.Label=Edge.Source; Project Edge;");
            IndexNestedLoopsJoins inlj = new IndexNestedLoopsJoins(1,nodeHeapFile, edgeHeapFile, edgeSourceLabelIndexFile, null,null, rid );
            RID innerRid = new RID();
            boolean indicate=true;
            while (true)
            {
                Edge edge = (Edge)inlj.get_next(innerRid);
                if (edge!=null)
                {
                    EID eid = new EID();
                    eid.pageNo.pid = innerRid.pageNo.pid;
                    eid.slotNo = innerRid.slotNo;
                    int edgeValue;
                    if(boundType==1)
                    {
                        edgeValue = 1;
                    }
                    else
                    {
                        edgeValue = edge.getWeight();
                    }
                    if((weight+ edgeValue)<=maxBound) {
                        indicate = false;
                        DFS(innerRid, 2, weight + edgeValue, boundType);
                    }
                }
                else {
                    if(indicate)
                    {
                        //write to outputFile
                        Tuple tuple = new Tuple();

                        NID tail = new NID();
                        tail.pageNo.pid = rid.pageNo.pid;
                        tail.slotNo = rid.slotNo;
                        Node tailNode = nodeHeapFile.getNode(tail);

                        //System.out.println("Tail Label:"+tailNode.getLabel());
                        AttrType[] types= new AttrType[1];
                        types[0] = new AttrType(AttrType.attrString);
                        short [] Ssizes = new short [1];
                        Ssizes[0] = 10;
                        tuple.setHdr((short)1,types,Ssizes);

                        String result = rootLabel+"_"+tailNode.getLabel();
                        tuple.setStrFld(1,result);
                        byte[] tempiter = tuple.getTupleByteArray();
                        outputFile.insertRecord(tempiter);
                    }
                    else
                        indicate=true;

                    break;
                }
            }
        }
        else    //Edge Node Join
        {
            System.out.println("QP: IndexNestedLoopJoin Edge|X|Node , Join Condition is Edge.Destination=Node.Label; Project Node");
            IndexNestedLoopsJoins inlj = new IndexNestedLoopsJoins(4,edgeHeapFile, nodeHeapFile, nodeIndexFile, null,null, rid );
            RID innerRid = new RID();
            while (true)
            {
                Node node = (Node) inlj.get_next(innerRid);
                if(node!=null)
                {
                    NID nid = new NID();
                    nid.pageNo.pid = innerRid.pageNo.pid;
                    nid.slotNo = innerRid.slotNo;


                    RID tempRid = new RID();
                    tempRid.pageNo.pid = nid.pageNo.pid;
                    tempRid.slotNo = nid.slotNo;
                    DFS((RID) tempRid, 1, weight, boundType);

                }
                else
                {
                    break;
                }
            }
        }
    }

    public void close() throws HFDiskMgrException, InvalidTupleSizeException, IOException, InvalidSlotNumberException, FileAlreadyDeletedException, HFBufMgrException {
        outputFilescan.close();
        outputFile.deleteFile();

    }

}
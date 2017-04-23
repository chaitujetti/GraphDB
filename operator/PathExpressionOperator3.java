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
    private int maxNumEdges;
    private int maxTotalEdgeWeight;
    private int condition;

    private NodeHeapfile nodeHeapFile;
    private EdgeHeapfile edgeHeapFile;
    private BTreeFile nodeIndexFile;
    private BTreeFile edgeSourceLabelIndexFile;

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

    public FileScan findTailNodes(int value) throws Exception {
        NID tempNID = new NID();
        tempNID.pageNo.pid = root.pageNo.pid;
        tempNID.slotNo = root.slotNo;
        Node rootNode = nodeHeapFile.getNode(tempNID);
        rootLabel=rootNode.getLabel();

        if(condition==1)
        {
            maxNumEdges=value;
            DFS_depth(root,1,0);
        }

        else
        {
            maxTotalEdgeWeight=value;
            DFS_weight(root,1,0);
        }


        FldSpec [] Sprojection = new FldSpec[1];
        Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);

        AttrType[] types= new AttrType[1];
        types[0] = new AttrType(AttrType.attrString);

        short [] Ssizes = new short [1];
        Ssizes[0] = 10;

        FileScan am = null;
        try {
            am  = new FileScan(outputFile._fileName, types, Ssizes,
                    (short)1, (short)1,
                    Sprojection, null);
        }
        catch (Exception e) {
            System.err.println (""+e);
        }
        return am;
    }

    public void DFS_weight(RID rid, int flag, int weight) throws Exception
    {
        if(flag==1) //Node Edge join
        {

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
                    if((weight+edge.getWeight())<maxTotalEdgeWeight) {
                        indicate = false;
                        DFS_weight(innerRid, 2, weight + edge.getWeight());
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

                      System.out.println("Tail Label:"+tailNode.getLabel());
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
                    DFS_weight((RID) tempRid, 1, weight);

                }
                else
                {
                    break;
                }
            }
        }
    }

    public void DFS_depth(RID rid, int flag, int depth) throws Exception
    {
        if(flag==1) //Node Edge join
        {

            IndexNestedLoopsJoins inlj = new IndexNestedLoopsJoins(1,nodeHeapFile, edgeHeapFile, edgeSourceLabelIndexFile, null,null, rid );
            RID innerRid = new RID();

            while (true)
            {
                Edge edge = (Edge)inlj.get_next(innerRid);
                if (edge!=null)
                {
                    EID eid = new EID();
                    eid.pageNo.pid = innerRid.pageNo.pid;
                    eid.slotNo = innerRid.slotNo;
                    DFS_depth(innerRid,2,depth+1);
                }
                else {
                    break;
                }
            }
        }
        else    //Edge Node Join
        {
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

                    if(depth<maxNumEdges) {
                        RID tempRid = new RID();
                        tempRid.pageNo.pid = nid.pageNo.pid;
                        tempRid.slotNo = nid.slotNo;
                        DFS_depth((RID) tempRid, 1, depth);
                    }
                    else
                    {
                        //write to outputFile
                        Tuple tuple = new Tuple();
                        System.out.println("TailNode's RID:"+Integer.toString(nid.pageNo.pid)+","+Integer.toString(nid.slotNo));
                        Node node1 = nodeHeapFile.getNode(nid);
                        System.out.println("Tail Label:"+node1.getLabel());
                        AttrType[] types= new AttrType[1];
                        types[0] = new AttrType(AttrType.attrString);
                        short [] Ssizes = new short [1];
                        Ssizes[0] = 10;
                        tuple.setHdr((short)1,types,Ssizes);

                        String result = rootLabel+"_"+node1.getLabel();
                        tuple.setStrFld(1,result);
                        byte[] tempiter = tuple.getTupleByteArray();
                        outputFile.insertRecord(tempiter);
                    }
                }
                else
                {
                    break;
                }
            }
        }
    }

}
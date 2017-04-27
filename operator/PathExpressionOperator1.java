package operator;

import btree.BTreeFile;
import heap.*;
import iterator.*;
import global.*;

import java.io.IOException;


public class PathExpressionOperator1 implements GlobalConst
{
    private NodeRegEx[] nodeRegEx;
    private RID root;
    private String rootLabel="";
    private int flag;

    private NodeHeapfile nodeHeapFile;
    private EdgeHeapfile edgeHeapFile;
    private BTreeFile nodeIndexFile;
    private BTreeFile edgeSourceLabelIndexFile;

    private FileScan outputFilescan;

    Heapfile outputFile;

    private int position;

    private short  stringSize;

    public PathExpressionOperator1(NodeRegEx[] nodeRegEx, RID firstNode, NodeHeapfile nodeHeapFile, EdgeHeapfile edgeHeapFile, BTreeFile nodeIndexFile, BTreeFile edgeSourceLabelIndexFile, String outputHeapFileName) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        this.nodeRegEx=nodeRegEx;
        root = new RID();
        root.pageNo.pid = firstNode.pageNo.pid;
        root.slotNo = firstNode.slotNo;
        this.nodeHeapFile = nodeHeapFile;
        this.edgeHeapFile = edgeHeapFile;
        this.nodeIndexFile = nodeIndexFile;
        this.edgeSourceLabelIndexFile = edgeSourceLabelIndexFile;
        flag=1;
        position =0;
        outputFile = new Heapfile(outputHeapFileName);
        outputFilescan = null;

        stringSize = 2 * MAX_STR_SIZE + 1;
    }

    public CondExpr[] setNodeExpressions(String label)
    {
        CondExpr[] expr = new CondExpr[2];
        expr[0] = new CondExpr();
        expr[0].next = null;
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);

        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),1);

        expr[0].type2 = new AttrType(AttrType.attrString);
        expr[0].operand2.string = label;

        expr[1]=null;
        return expr;
    }

    public CondExpr[] setNodeExpressions(Descriptor desc)
    {
        CondExpr[] expr = new CondExpr[2];
        expr[0] = new CondExpr();
        expr[0].next = null;
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);

        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),2);

        expr[0].type2 = new AttrType(AttrType.attrDesc);
        expr[0].operand2.descriptor = desc;

        expr[1]=null;
        return expr;

    }

    public FileScan getOutputFileScanObject()
    {
        FldSpec [] Sprojection = new FldSpec[1];
        Sprojection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);

        AttrType[] types= new AttrType[1];
        types[0] = new AttrType(AttrType.attrString);

        short [] Ssizes = new short [1];
        Ssizes[0] = stringSize;

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
    public void findTailNodes() throws Exception {
        NID tempNID = new NID();
        tempNID.pageNo.pid = root.pageNo.pid;
        tempNID.slotNo = root.slotNo;
        Node rootNode = nodeHeapFile.getNode(tempNID);
        rootLabel=rootNode.getLabel();

        DFS(root,1,0);

    }

    public void DFS(RID rid, int flag, int pos) throws Exception
    {
        if(flag==1) //Node Edge join
        {
            System.out.println("QP: IndexNestedLoopJoin Node|X|Edge Join Condition is Node.Label=Edge.Source; Project Edge;");
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
                    DFS(innerRid,2,pos);
                }
                else {
                    break;
                }
            }
            inlj.close();
        }
        else    //Edge Node Join
        {
            NodeRegEx token = nodeRegEx[pos];
            CondExpr[] innerCond;
            if(token.getDesc()==null)
            {
                System.out.println("QP: IndexNestedLoopJoin Edge|X|Node , Join Condition is Edge.Destination=Node.Label; Select Node where Label = "+token.getLabel()+"; Project Node;");
                innerCond = setNodeExpressions(token.getLabel());
            }
            else
            {
                System.out.println("QP: IndexNestedLoopJoin Edge|X|Node , Join Condition is Edge.Destination=Node.Label; Select Node where Descriptor = ["+token.getDesc().get(0)+","+token.getDesc().get(1)+","+token.getDesc().get(2)+","+token.getDesc().get(3)+","+token.getDesc().get(4)+"]; Project Node");
                innerCond = setNodeExpressions(token.getDesc());
            }
            IndexNestedLoopsJoins inlj = new IndexNestedLoopsJoins(4,edgeHeapFile, nodeHeapFile, nodeIndexFile, null,innerCond, rid );
            RID innerRid = new RID();
            while (true)
            {
                Node node = (Node) inlj.get_next(innerRid);
                if(node!=null)
                {
                    NID nid = new NID();
                    nid.pageNo.pid = innerRid.pageNo.pid;
                    nid.slotNo = innerRid.slotNo;
                    if(!(pos==nodeRegEx.length-1))
                    {
                        RID tempRid = new RID();
                        tempRid.pageNo.pid = nid.pageNo.pid;
                        tempRid.slotNo = nid.slotNo;
                        DFS(tempRid, 1, pos + 1);
                    }
                    else
                    {
                        //write to outputFile
                        Tuple tuple = new Tuple();
                        //System.out.println("TailNode's RID:"+Integer.toString(nid.pageNo.pid)+","+Integer.toString(nid.slotNo));
                        Node node1 = nodeHeapFile.getNode(nid);
                        //System.out.println("Tail Label:"+node1.getLabel());
                        AttrType[] types= new AttrType[1];
                        types[0] = new AttrType(AttrType.attrString);
                        short [] Ssizes = new short [1];
                        Ssizes[0] = stringSize;
                        tuple.setHdr((short)1,types,Ssizes);

                        String result = rootLabel+"_"+node1.getLabel();
                        //System.out.println(result);
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
            inlj.close();
        }

    }

    public void close() throws HFDiskMgrException, InvalidTupleSizeException, IOException, InvalidSlotNumberException, FileAlreadyDeletedException, HFBufMgrException {
        try {
            outputFilescan.close();
            outputFile.deleteFile();
        }
        catch (Exception e) {
            e.printStackTrace();
        }

    }

}
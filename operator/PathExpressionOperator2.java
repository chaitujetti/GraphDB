package operator;

import btree.BTreeFile;
import heap.*;
import iterator.*;
import global.*;

import java.io.IOException;


public class PathExpressionOperator2
{
    private EdgeRegEx[] edgeRegEx;
    private RID root;
    private int flag;

    private NodeHeapfile nodeHeapFile;
    private EdgeHeapfile edgeHeapFile;
    private BTreeFile nodeIndexFile;
    private BTreeFile edgeSourceLabelIndexFile;

    Heapfile outputFile;

    private int position;

    public PathExpressionOperator2(EdgeRegEx[] edgeRegEx, RID firstNode, NodeHeapfile nodeHeapFile, EdgeHeapfile edgeHeapFile, BTreeFile nodeIndexFile, BTreeFile edgeSourceLabelIndexFile, String outputHeapFileName) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        this.edgeRegEx=edgeRegEx;
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
    }

    public CondExpr[] setEdgeExpressions(String label)
    {
        //0 - edge condition based on edge label
        CondExpr[] expr = new CondExpr[1];
        expr[0] = new CondExpr();
        expr[0].next = null;
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);

        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),1);

        expr[0].type2 = new AttrType(AttrType.attrString);
        expr[0].operand2.string = label;

        return expr;
    }

    public CondExpr[] setEdgeExpressions(int weight)
    {
        CondExpr[] expr = new CondExpr[1];
        expr[0] = new CondExpr();
        expr[0].next = null;
        expr[0].op = new AttrOperator(AttrOperator.aopLE);

        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),4);

        expr[0].type2 = new AttrType(AttrType.attrInteger);
        expr[0].operand2.integer = weight;

        return expr;
    }


    public Scan findTailNodes() throws Exception {
        DFS(root,1,0);
        Scan scan = new Scan(outputFile);
        return scan;
    }

    public void DFS(RID rid, int flag, int pos) throws Exception
    {
        if(flag==1) //Node Edge join
        {
            EdgeRegEx token = edgeRegEx[pos];
            CondExpr[] innerCond;
            if(token.getLabel()==null)
            {
                innerCond = setEdgeExpressions(token.getMax_edge_weight());
            }
            else
            {
                innerCond = setEdgeExpressions(token.getLabel());
            }

            IndexNestedLoopsJoins inlj = new IndexNestedLoopsJoins(1,nodeHeapFile, edgeHeapFile, edgeSourceLabelIndexFile, null,innerCond, rid );
            RID innerRid = new RID();
            while (true)
            {
                Edge edge = (Edge)inlj.get_next(innerRid);
                if (edge!=null)
                {
                    EID eid = new EID();
                    eid.pageNo.pid = innerRid.pageNo.pid;
                    eid.slotNo = innerRid.slotNo;
                    //DFS((RID)eid,2,pos);
                    DFS(innerRid,2,pos);
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
                    if(edgeRegEx[pos+1]!=null) {
                        DFS((RID) nid, 1, pos + 1);
                    }
                    else
                    {
                        //write to outputFile
                        Tuple tuple = new Tuple();
                        tuple.setIntFld(1,nid.pageNo.pid);
                        tuple.setIntFld(2,nid.slotNo);
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
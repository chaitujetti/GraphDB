package query;

import btree.BTreeFile;
import diskmgr.GraphDB;
import global.*;
import heap.*;
import iterator.Sort;
import iterator.FileScan;
import operator.EdgeRegEx;
import operator.NodeRegEx;
import operator.PathExpressionOperator2;

import java.io.IOException;

/**
 * Created by ankur on 4/20/17.
 */
public class PathExpressionQuery2 {

    private EdgeRegEx[] nodePathExp;
    Heapfile output;
    NodeHeapfile nhf;
    EdgeHeapfile ehf;
    BTreeFile nodeIndexFile;
    BTreeFile edgeSourceLabelsIndexFile;
    NodeRegEx firstNode;

    public PathExpressionQuery2(String[] input, GraphDB graphDB) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        parseInput(input);
        nhf = graphDB.getNhf();
        ehf = graphDB.getEhf();
        nodeIndexFile = graphDB.nodeLabels_BFile;
        edgeSourceLabelsIndexFile = graphDB.edgeSourceLabels_BFile;
    }

    public void parseInput(String[] input)
    {

        if(input[0].startsWith("L:"))
        {
            String label = input[0].split(":")[1];
            NodeRegEx node = new NodeRegEx(label);
            firstNode=node;
        }
        else
        {
            String descriptorStr = input[0].split(":")[1];
            String[] tokens = descriptorStr.split(",");
            short[] vals = new short[5];
            for(int j=0;j<5;j++)
            {
                vals[j]=Short.parseShort(tokens[j]);
                System.out.println(vals[j]);
            }
            Descriptor desc = new Descriptor();
            desc.set(vals[0],vals[1],vals[2],vals[3],vals[4]);
            NodeRegEx node = new NodeRegEx(desc);
            firstNode=node;
        }

        int numEdges = input.length-1;
        nodePathExp = new EdgeRegEx[numEdges];
        for(int i=0;i<numEdges;i++)
        {
            if(input[i+1].startsWith("L:"))
            {
                String label = input[i+1].split(":")[1];
                EdgeRegEx edge = new EdgeRegEx(label);
                nodePathExp[i]=edge;
            }
            else
            {
                String maxEdgeWeight = input[i+1].split(":")[1];
                EdgeRegEx edge = new EdgeRegEx(Integer.parseInt(maxEdgeWeight));
                nodePathExp[i]=edge;
            }
        }
    }

    public void fetchAllTailLabels() throws Exception {
        if (firstNode.getLabel()==null)
        {
            NScan nscan = nhf.openScan();
            Descriptor firstNodeDesc = firstNode.getDesc();
            NID root = new NID();
            Node node;
            node = nscan.getNext(root);
            while (node!=null)
            {
                double isEquals = firstNodeDesc.equal(node.getDesc());
                System.out.println(node.getDesc().get(0)+" "+node.getDesc().get(1)+" "+node.getDesc().get(2)+" "+node.getDesc().get(3)+" "+node.getDesc().get(4));
                if(isEquals==1)//true
                {
                    RID tempRID = new RID();
                    tempRID.pageNo.pid = root.pageNo.pid;
                    tempRID.slotNo = root.slotNo;
                    PathExpressionOperator2 pe2 = new PathExpressionOperator2(nodePathExp, tempRID,nhf, ehf, nodeIndexFile, edgeSourceLabelsIndexFile, "TemporaryOutput");
                    FileScan tempFileScan = pe2.findTailNodes();

                    AttrType[] types= new AttrType[1];
                    types[0] = new AttrType(AttrType.attrString);

                    short [] Ssizes = new short [1];
                    Ssizes[0] = 10;
                    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);

                    Sort sort_nodes = null;
                    try {
                        sort_nodes = new Sort (types,(short)1, Ssizes, tempFileScan, 1, ascending, Ssizes[0], 10);
                    }
                    catch (Exception e) {
                        System.err.println (""+e);
                    }


                    Tuple t;
                    while ((t = sort_nodes.get_next()) != null) {
                        System.out.println("Sorted: "+t.getStrFld(1));
                    }
                }
                node = nscan.getNext(root);
            }
        }

        else
        {
            NScan nscan = nhf.openScan();
            String firstNodeLabel = firstNode.getLabel();
            NID root = new NID();
            Node node;
            node = nscan.getNext(root);
            while (node!=null)
            {
                //System.out.println(node.getLabel());
                //node.print();
                if(firstNodeLabel.equals(node.getLabel()))//true
                {
                    RID tempRID = new RID();
                    tempRID.pageNo.pid = root.pageNo.pid;
                    tempRID.slotNo = root.slotNo;
                    System.out.println("Temp RID:"+Integer.toString(tempRID.pageNo.pid)+","+Integer.toString(tempRID.slotNo));
                    PathExpressionOperator2 pe2 = new PathExpressionOperator2(nodePathExp,tempRID,nhf, ehf, nodeIndexFile, edgeSourceLabelsIndexFile, "TemporaryOutput");
                    FileScan tempFileScan = pe2.findTailNodes();

                    AttrType[] types= new AttrType[1];
                    types[0] = new AttrType(AttrType.attrString);

                    short [] Ssizes = new short [1];
                    Ssizes[0] = 10;
                    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);

                    Sort sort_nodes = null;
                    try {
                        sort_nodes = new Sort (types,(short)1, Ssizes, tempFileScan, 1, ascending, Ssizes[0], 10);
                    }
                    catch (Exception e) {
                        System.err.println (""+e);
                    }


                    Tuple t;
                    while ((t = sort_nodes.get_next()) != null) {
                        System.out.println("Sorted: "+t.getStrFld(1));
                    }
                    break; //////Should be there in both query types
                }
                node = nscan.getNext(root);
            }
        }
    }
}

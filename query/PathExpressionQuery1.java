package query;
import btree.BTreeFile;
import bufmgr.PageNotReadException;
import diskmgr.GraphDB;
import global.*;
import heap.*;
import iterator.*;
import operator.*;

import java.io.IOException;
import java.util.Arrays;

/**
 * Created by bharath on 4/17/17.
 */
public class PathExpressionQuery1 {
    private NodeRegEx[] nodePathExp;
    Heapfile output;
    NodeHeapfile nhf;
    EdgeHeapfile ehf;
    BTreeFile nodeIndexFile;
    BTreeFile edgeSourceLabelsIndexFile;

    public PathExpressionQuery1(String[] input, GraphDB graphDB) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        parseInput(input);
        nhf = graphDB.getNhf();
        ehf = graphDB.getEhf();
        nodeIndexFile = graphDB.nodeLabels_BFile;
        edgeSourceLabelsIndexFile = graphDB.edgeSourceLabels_BFile;
    }

    public void parseInput(String[] input)
    {
        int numNodes = input.length;
        nodePathExp = new NodeRegEx[numNodes];
        for(int i=0;i<numNodes;i++)
        {
            if(input[i].startsWith("L:"))
            {
                String label = input[i].split(":")[1];
                NodeRegEx node = new NodeRegEx(label);
                nodePathExp[i]=node;
            }
            else
            {
                String descriptorStr = input[i].split(":")[1];
                String[] tokens = descriptorStr.split(",");
                short[] vals = new short[5];
                for(int j=0;j<5;j++)
                {
                    vals[j]=Short.parseShort(tokens[j]);
                }
                Descriptor desc = new Descriptor();
                desc.set(vals[0],vals[1],vals[2],vals[3],vals[4]);
                NodeRegEx node = new NodeRegEx(desc);
                nodePathExp[i]=node;
            }
        }
    }

    public void projectResult(FileScan tempFileScan, String queryType) throws Exception {
        if(queryType.equals("a"))
        {
            System.out.println("Unsorted Output");
            Tuple t;
            while (tempFileScan!=null && (t=tempFileScan.get_next())!=null) {
                System.out.println(t.getStrFld(1));
            }
        }

        if(queryType.equals("b")||queryType.equals("c"))
        {
            AttrType[] types = new AttrType[1];
            types[0] = new AttrType(AttrType.attrString);

            short[] Ssizes = new short[1];
            Ssizes[0] = 10;
            TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
            Sort sort_nodes = null;
            try {
                sort_nodes = new Sort(types, (short) 1, Ssizes, tempFileScan, 1, ascending, Ssizes[0], 10);
            } catch (Exception e) {
                System.err.println("Error in Sort:" + e);
            }

            if(queryType.equals("b")) {
                System.out.println("Sorted");
                Tuple t;
                while ((t = sort_nodes.get_next()) != null) {
                    System.out.println(t.getStrFld(1));
                }
            }

            if(queryType.equals("c")) {
                System.out.println("Distinct");
                Tuple t;
                String previousValue="";
                while ((t = sort_nodes.get_next()) != null) {
                    String currentValue = t.getStrFld(1);
                    if(!currentValue.equals(previousValue)) {
                        System.out.println(currentValue);
                        previousValue=currentValue;
                    }
                }
            }
            sort_nodes.close();
        }
        tempFileScan.close();

    }
    public void fetchAllTailLabels(String queryType) throws Exception {
        NodeRegEx firstNode = nodePathExp[0];
        NodeRegEx[] nodeRegExFromSecond = Arrays.copyOfRange(nodePathExp, 1, nodePathExp.length);
        if (firstNode.getLabel()==null)
        {
            NScan nscan = nhf.openScan();
            Descriptor firstNodeDesc = firstNode.getDesc();
            NID root = new NID();
            Node node;
            node = nscan.getNext(root);
            //FileScan tempFileScan=null;
            PathExpressionOperator1 pe1=null;
            while (node!=null)
            {
                double isEquals = firstNodeDesc.equal(node.getDesc());
                if(isEquals==1)//true
                {
                    RID tempRID = new RID();
                    tempRID.pageNo.pid = root.pageNo.pid;
                    tempRID.slotNo = root.slotNo;
                    //System.out.println("Incoming Label:"+node.getLabel());
                    pe1 = new PathExpressionOperator1(nodeRegExFromSecond, tempRID,nhf, ehf, nodeIndexFile, edgeSourceLabelsIndexFile, "TemporaryOutput");
//                    FileScan tempFileScan = pe1.findTailNodes();
                    pe1.findTailNodes();

                    AttrType[] types= new AttrType[1];
                    types[0] = new AttrType(AttrType.attrString);

                    short [] Ssizes = new short [1];
                    Ssizes[0] = 10;
                    TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
                }
                node = nscan.getNext(root);
            }
            projectResult(pe1.getOutputFileScanObject(),queryType);
            pe1.close();
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
                if(firstNodeLabel.equals(node.getLabel()))//true
                {
                    RID tempRID = new RID();
                    tempRID.pageNo.pid = root.pageNo.pid;
                    tempRID.slotNo = root.slotNo;
                    //System.out.println("Temp RID:"+Integer.toString(tempRID.pageNo.pid)+","+Integer.toString(tempRID.slotNo));
                    PathExpressionOperator1 pe1 = new PathExpressionOperator1(nodeRegExFromSecond,tempRID,nhf, ehf, nodeIndexFile, edgeSourceLabelsIndexFile, "TemporaryOutput");
                    pe1.findTailNodes();
                    projectResult(pe1.getOutputFileScanObject(),queryType);
                    pe1.close();
                    break; //////Should be there in both query types
                }
                node = nscan.getNext(root);
            }
        }
    }
}

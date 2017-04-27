package query;

import btree.BTreeFile;
import diskmgr.GraphDB;
import global.*;
import heap.*;
import iterator.Sort;
import iterator.FileScan;
import operator.NodeRegEx;
import operator.PathExpressionOperator3;

import java.io.IOException;

/**
 * Created by ankur on 4/22/17.
 */
public class PathExpressionQuery3 implements GlobalConst{

    private int condition;
    private int value;
    private GraphDB graphDB;
    NodeHeapfile nhf;
    EdgeHeapfile ehf;
    BTreeFile nodeIndexFile;
    BTreeFile edgeSourceLabelsIndexFile;
    NodeRegEx firstNode;

    private short  stringSize;

    public PathExpressionQuery3(String[] input, GraphDB graphDB) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        parseInput(input);
        this.graphDB = graphDB;
        nhf = graphDB.getNhf();
        ehf = graphDB.getEhf();
        nodeIndexFile = graphDB.nodeLabels_BFile;
        edgeSourceLabelsIndexFile = graphDB.edgeSourceLabels_BFile;
        stringSize = MAX_STR_SIZE;
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
                //System.out.println(vals[j]);
            }
            Descriptor desc = new Descriptor();
            desc.set(vals[0],vals[1],vals[2],vals[3],vals[4]);
            NodeRegEx node = new NodeRegEx(desc);
            firstNode=node;
        }

        String temp = input[1].split(":")[1];
        value=Integer.parseInt(temp);

        if(input[1].startsWith("D:"))
            condition=1;
        else
            condition=2;
    }

    public void projectResult(FileScan tempFileScan, String queryType) throws Exception {
        if(queryType.equals("a"))
        {
            System.out.println("QP: Project Head and Tail Nodes");
            Tuple t;
            while (tempFileScan!=null && (t=tempFileScan.get_next())!=null) {
                System.out.println(t.getStrFld(1));
            }

        }

        if(queryType.equals("b")||queryType.equals("c"))
        {
            System.out.println("QP: Sort Head and Tail Nodes in Ascending order");
            AttrType[] types = new AttrType[1];
            types[0] = new AttrType(AttrType.attrString);

            short[] Ssizes = new short[1];
            Ssizes[0] = stringSize;
            TupleOrder ascending = new TupleOrder(TupleOrder.Ascending);
            Sort sort_nodes = null;
            try {
                sort_nodes = new Sort(types, (short) 1, Ssizes, tempFileScan, 1, ascending, Ssizes[0], 10);
            } catch (Exception e) {
                System.err.println("Error in Sort:" + e);
            }
            //System.out.println("No. of Disk pages read:"+graphDB.getNoOfReads()+"; No. of Disk Pages written:"+graphDB.getNoOfWrites());

            if(sort_nodes!=null) {
                if (queryType.equals("b")) {
                    System.out.println("QP: Project Head and Tail Nodes");
                    Tuple t;
                    while ((t = sort_nodes.get_next()) != null) {
                        System.out.println(t.getStrFld(1));
                    }
                    //System.out.println("No. of Disk pages read:"+graphDB.getNoOfReads()+"; No. of Disk Pages written:"+graphDB.getNoOfWrites());
                }

                if (queryType.equals("c")) {
                    System.out.println("QP: Project Distinct pairs of Head and Tail Nodes");
                    Tuple t;
                    String previousValue = "";
                    while ((t = sort_nodes.get_next()) != null) {
                        String currentValue = t.getStrFld(1);
                        if (!currentValue.equals(previousValue)) {
                            System.out.println(currentValue);
                            previousValue = currentValue;
                        }
                    }
                    //System.out.println("No. of Disk pages read:"+graphDB.getNoOfReads()+"; No. of Disk Pages written:"+graphDB.getNoOfWrites());
                }
            }
            else {
                System.out.println("Projection failed as Sort failed");
            }
            try {
                sort_nodes.close();
            }
            catch (Exception e) {
                //e.printStackTrace();
                System.out.println("Sort done successfully: Close failed");
            }
        }
        try {
            tempFileScan.close();
        }
        catch (Exception e) {
            //e.printStackTrace();
            System.out.println("Filescan close failed");
        }

    }


    public void fetchAllTailLabels(String queryType) throws Exception {
        if (firstNode.getLabel()==null)
        {
            NScan nscan = nhf.openScan();
            Descriptor firstNodeDesc = firstNode.getDesc();
            NID root = new NID();
            Node node;
            node = nscan.getNext(root);
            PathExpressionOperator3 pe3 = null;
            System.out.println("QP: Select Node where Descriptor=["+firstNodeDesc.get(0)+","+firstNodeDesc.get(1)+","+firstNodeDesc.get(2)+","+firstNodeDesc.get(3)+","+firstNodeDesc.get(4)+"]");
            while (node!=null)
            {
                double isEquals = firstNodeDesc.equal(node.getDesc());
                if(isEquals==1)//true
                {
                    RID tempRID = new RID();
                    tempRID.pageNo.pid = root.pageNo.pid;
                    tempRID.slotNo = root.slotNo;
                    pe3 = new PathExpressionOperator3(condition,tempRID,nhf, ehf, nodeIndexFile, edgeSourceLabelsIndexFile, "TemporaryOutput");
                    pe3.findTailNodes(value);
                }
                node = nscan.getNext(root);
            }
            System.out.println("No. of Disk pages read:"+graphDB.getNoOfReads()+"; No. of Disk Pages written:"+graphDB.getNoOfWrites());
            graphDB.flushCounters();
            if(pe3!=null) {
                projectResult(pe3.getOutputFileScanObject(), queryType);
                pe3.close();
            }
            System.out.println("No. of Disk pages read:"+graphDB.getNoOfReads()+"; No. of Disk Pages written:"+graphDB.getNoOfWrites());
            graphDB.flushCounters();
            nscan.closescan();
        }

        else
        {
            NScan nscan = nhf.openScan();
            String firstNodeLabel = firstNode.getLabel();
            NID root = new NID();
            Node node;
            node = nscan.getNext(root);
            PathExpressionOperator3 pe3 = null;
            System.out.println("QP: Select Node where Label="+firstNodeLabel);
            while (node!=null)
            {
                //System.out.println(firstNodeLabel+" "+node.getLabel());
                if(firstNodeLabel.equals(node.getLabel()))//true
                {
                    RID tempRID = new RID();
                    tempRID.pageNo.pid = root.pageNo.pid;
                    tempRID.slotNo = root.slotNo;
                    //System.out.println("Temp RID:"+Integer.toString(tempRID.pageNo.pid)+","+Integer.toString(tempRID.slotNo));
                    pe3 = new PathExpressionOperator3(condition,tempRID,nhf, ehf, nodeIndexFile, edgeSourceLabelsIndexFile, "TemporaryOutput");
                    pe3.findTailNodes(value);
                    System.out.println("No. of Disk pages read:"+graphDB.getNoOfReads()+"; No. of Disk Pages written:"+graphDB.getNoOfWrites());
                    graphDB.flushCounters();
                    projectResult(pe3.getOutputFileScanObject(),queryType);
                    pe3.close();
                    System.out.println("No. of Disk pages read:"+graphDB.getNoOfReads()+"; No. of Disk Pages written:"+graphDB.getNoOfWrites());
                    graphDB.flushCounters();
                    break; //////Should be there in both query types
                }
                node = nscan.getNext(root);
            }
            nscan.closescan();
        }
    }
}

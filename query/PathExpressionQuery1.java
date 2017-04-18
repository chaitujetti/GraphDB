package query;
import btree.BTreeFile;
import diskmgr.GraphDB;
import global.*;
import heap.*;
import operator.*;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

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
//    private String[] input;

    public PathExpressionQuery1(String[] input, GraphDB graphDB) throws IOException, HFException, HFBufMgrException, HFDiskMgrException {
        parseInput(input);
//        output = new Heapfile("Query1");
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

    public void fetchAllTailLabels() throws Exception {
        NodeRegEx firstNode = nodePathExp[0];
        NodeRegEx[] nodeRegExFromSecond = Arrays.copyOfRange(nodePathExp, 1, nodePathExp.length);
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
                if(isEquals==1)//true
                {
                    RID tempRID = new RID();
                    tempRID.pageNo.pid = root.pageNo.pid;
                    tempRID.slotNo = root.slotNo;
                    PathExpressionOperator1 pe1 = new PathExpressionOperator1(nodeRegExFromSecond, tempRID,nhf, ehf, nodeIndexFile, edgeSourceLabelsIndexFile, "TemporaryOutput");
                    Scan tempFileScan = pe1.findTailNodes();
                    RID rid = new RID();
                    Tuple tuple = tempFileScan.getNext(rid);
                    NID tempNID = new NID();
                    tempNID.pageNo.pid = tuple.getIntFld(1);
                    tempNID.slotNo = tuple.getIntFld(2);
                    Node eachNode = nhf.getNode(tempNID);
                    System.out.println(eachNode.getLabel());
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
                    PathExpressionOperator1 pe1 = new PathExpressionOperator1(nodeRegExFromSecond,tempRID,nhf, ehf, nodeIndexFile, edgeSourceLabelsIndexFile, "TemporaryOutput");
                    Scan tempFileScan = pe1.findTailNodes();
                    RID rid = new RID();
                    Tuple tuple = tempFileScan.getNext(rid);
                    AttrType[] types= new AttrType[2];
                    types[0] = new AttrType(AttrType.attrInteger);
                    types[1] = new AttrType(AttrType.attrInteger);
                    while (tuple!=null) {
                        tuple.setHdr((short)2,types,null);
                        NID tempNID = new NID();
                        tempNID.pageNo.pid = tuple.getIntFld(1);
                        tempNID.slotNo = tuple.getIntFld(2);
                        Node eachNode = nhf.getNode(tempNID);
                        System.out.println("Tail Label from file:" + eachNode.getLabel());
                        tuple=tempFileScan.getNext(rid);
                    }
                }
                node = nscan.getNext(root);
            }
        }
    }
}

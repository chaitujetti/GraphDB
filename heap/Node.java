package heap;

/**
 * Created by vamsikrishnag on 3/7/17.
 */

import java.io.*;
import java.lang.*;
import global.*;

public class Node extends Tuple
{
    private String label;
    private Descriptor desc;

    public static final AttrType[] types = {new AttrType(AttrType.attrString),new AttrType(AttrType.attrDesc)};
    public static final short[] sizes = {LABEL_MAX_LENGTH,10};
    public static final short numFld = 2;

    public Node() throws IOException, InvalidTypeException, InvalidTupleSizeException
    {
        super();
        setHdr(numFld, types, sizes);
    }

    public Node(Tuple tuple) throws IOException, InvalidTypeException, InvalidTupleSizeException
    {
        this.data = tuple.data;
        setHdr(numFld, types, sizes);
    }

    public Node(Node node)
    {
        super(node);
        this.desc = node.desc;
        this.label = node.label;
    }

    public Node(byte [] atuple, int offset) throws IOException, InvalidTypeException, InvalidTupleSizeException
    {
        super(atuple,offset,atuple.length);
        setHdr(numFld, types, sizes);
    }

    public Node(byte [] atuple, int offset, int length) throws IOException, InvalidTypeException, InvalidTupleSizeException
    {
        super(atuple,offset,length);
        setHdr(numFld, types, sizes);
    }

    public byte [] getNodeByteArray()
    {
        return getTupleByteArray();
    }

    public short[] copyFldOffset() {

        return copyFldOffset();
    }

    public String getLabel() throws IOException, FieldNumberOutOfBoundException
    {
        return getStrFld(1);
    }

    public Descriptor getDesc() throws IOException, FieldNumberOutOfBoundException
    {
        return getDescFld(2);
    }

    public Node setLabel(String label) throws IOException, FieldNumberOutOfBoundException
    {
        this.label = label;
        setStrFld(1, this.label);
        return this;
    }

    public Node setDesc(Descriptor desc) throws IOException, FieldNumberOutOfBoundException
    {
        this.desc = desc;
        setDescFld(2, this.desc);
        return this;
    }

    private int getNodeLength() {
        return getLength();
    }
    
    public void print() throws IOException, FieldNumberOutOfBoundException
    {
        this.desc = getDescFld(2);
        this.label = getStrFld(1);
        System.out.println("[Label: "+ this.label + ", Descriptors: "+ this.desc.get(0) + ", " + this.desc.get(1) + ", " + this.desc.get(2) + ", " + this.desc.get(3) + ", " + this.desc.get(4) +"]");
    }

    public void nodeCopy(Node node)
    {
        byte [] temp = node.getTupleByteArray();
        System.arraycopy(temp, 0, data, getOffset(), getLength());
    }

    public void nodeInit(byte [] anode, int offset)
    {
        tupleInit(anode,offset,anode.length);
    }

    public void nodeSet(byte [] fromnode, int offset)
    {
        tupleSet(fromnode,offset,fromnode.length);
    }
}

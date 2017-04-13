package heap;

/**
 * Created by vamsikrishnag on 3/7/17.
 */

import java.io.*;
import java.lang.*;
import global.*;

public class Edge extends Tuple {
    private String label;
    private String sourceLabel;
    private String destinationLabel;
    private int weight;
    private NID source;
    private NID destination;
    public static final AttrType[] types = { new AttrType(AttrType.attrString), new AttrType(AttrType.attrString), new AttrType(AttrType.attrString), new AttrType(AttrType.attrInteger),
            new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger), new AttrType(AttrType.attrInteger)};
    public static final short[] sizes = { LABEL_MAX_LENGTH, LABEL_MAX_LENGTH, LABEL_MAX_LENGTH, 4,4,4,4,4};
    public static final short numFld = 8;

    
    public Edge() throws IOException, InvalidTypeException, InvalidTupleSizeException
    {
        super();
        setHdr(numFld, types, sizes);
    }

    public Edge(byte[] anode, int offset) throws IOException, InvalidTypeException, InvalidTupleSizeException
    {
        super(anode, offset, anode.length);
        setHdr(numFld, types, sizes);
    }
    public Edge(byte[] anode, int offset, int length) throws IOException, InvalidTypeException, InvalidTupleSizeException
    {
        super(anode, offset, length);
        setHdr(numFld, types, sizes);
    }

    public Edge(Edge e) throws IOException, InvalidTypeException, InvalidTupleSizeException
    {
        super(e);
        setHdr(numFld, types, sizes);
        this.sourceLabel=e.sourceLabel;
        this.destinationLabel=e.destinationLabel;
        this.label=e.label;
        this.weight=e.weight;
        this.source=e.source;
        this.destination=e.destination;
    }

    public Edge(Tuple tuple) throws IOException, InvalidTypeException, InvalidTupleSizeException
    {
        setHdr(numFld, types, sizes);
        if (tuple != null) {
            this.data = tuple.data;
        }
    }

    public byte[] getEdgeByteArray()
    {
        return getTupleByteArray();
    }

    private int getEdgeLength() 
    {
        return getLength();
    }

    public String getSourceLabel()  throws IOException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException
    {
        return getStrFld(1);
    }

    public String getDestinationLabel()  throws IOException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException
    {
        return getStrFld(2);
    }

    public String getLabel() throws IOException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException
    {
        return getStrFld(3);
    }

    public int getWeight() throws IOException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException
    {
        return getIntFld(4);
    }

    public NID getSource() throws IOException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException
    {
        NID sou = new NID();
        sou.pageNo.pid = getIntFld(5);
        sou.slotNo = getIntFld(6);
        return sou;
    }

    public NID getDestination() throws IOException, InvalidTypeException, InvalidTupleSizeException, FieldNumberOutOfBoundException
    {
        NID des = new NID();
        des.pageNo.pid = getIntFld(7);
        des.slotNo = getIntFld(8);
        return des;
    }

    public Edge setSourceLabel(String label) throws IOException, FieldNumberOutOfBoundException, FieldNumberOutOfBoundException 
    {
        this.label=label;
        setStrFld(1, this.label);
        return this;
    }

    public Edge setDestinationLabel(String label) throws IOException, FieldNumberOutOfBoundException {
        this.label=label;
        setStrFld(2, this.label);
        return this;
    }

    public Edge setLabel(String label) throws IOException, FieldNumberOutOfBoundException {
        this.label=label;
        setStrFld(3, this.label);
        return this;
    }

    public Edge setWeight(int Weight) throws IOException, FieldNumberOutOfBoundException {
        this.weight=Weight;
        setIntFld(4, this.weight);
        return this;
    }

    public Edge setSource(NID source) throws IOException, FieldNumberOutOfBoundException {
        this.source=source;
        setIntFld(5, this.source.pageNo.pid);
        setIntFld(6, this.source.slotNo);
        return this;
    }

    public Edge setDestination(NID dest) throws IOException, FieldNumberOutOfBoundException {
        this.destination=dest;
        setIntFld(7, this.destination.pageNo.pid);
        setIntFld(8, this.destination.slotNo);
        return this;
    }

    public Edge setSourceNodeLabel(String sourceNodeLabel) throws IOException {
        this.sourceLabel = sourceNodeLabel;
        Convert.setStrValue(this.sourceLabel, 30, data);
        tuple_length = getEdgeLength();
        return this;
    }

    public Edge setDestinationNodeLabel(String destinationNodeLabel) throws IOException {
        this.destinationLabel = destinationNodeLabel;
        Convert.setStrValue(this.destinationLabel, 40, data);
        tuple_length = getEdgeLength();
        return this;
    }

    public void print() throws IOException
    {
        System.out.print("[");
        System.out.print("source : "+this.sourceLabel);
        System.out.print("Destination : "+this.destinationLabel);
        System.out.print("edge label : "+this.label);
        System.out.print("weight : "+this.weight);
        System.out.println("]");
    }

    public void edgeCopy(Edge edge)
    {
        byte [] temp = edge.getEdgeByteArray();
        System.arraycopy(temp, 0, data, tuple_offset, tuple_length);

    }

    public void edgeInit(byte[] aedge, int offset)
    {
        tupleInit(aedge, offset, aedge.length);

    }
    public void edgeSet(byte[] fromedge, int offset)
    {
        tupleSet(fromedge, offset, fromedge.length);

    }

}

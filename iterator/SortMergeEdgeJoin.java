package iterator;

import com.sun.org.apache.xpath.internal.operations.String;
import global.AttrOperator;
import global.AttrType;
import global.Descriptor;
import global.TupleOrder;
import heap.Tuple;

/**
 * Created by vamsikrishnag on 4/12/17.
 */
public class SortMergeEdgeJoin {
    private CondExpr OutFilter[];
    private SortMerge s;

    public SortMergeEdgeJoin(String filename) {
        OutFilter = new CondExpr[3];
        for (int i=0;i<3;i++) {
            OutFilter[i] = new CondExpr();
        }
        setCondExpr(OutFilter);
        SortMergeJoin(filename);
    }

    public SortMergeEdgeJoin(String label, String filename) {
        OutFilter = new CondExpr[3];
        for (int i=0;i<3;i++) {
            OutFilter[i] = new CondExpr();
        }
        setCondExpr(OutFilter,label);
        SortMergeJoin(filename);
    }

    private static void setCondExpr(CondExpr[] expr) {
        expr[0].next = null;
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),7);
        expr[0].operand2.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),8);
        expr[1] = null;
        expr[2] = null;
    }

    private static void  setCondExpr(CondExpr[] expr, String label) {
        expr[0].next = null;
        expr[0].op = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec(new RelSpec(RelSpec.outer),7);
        expr[0].operand2.string = ""+label;

        expr[1].next = null;
        expr[1].op = new AttrOperator(AttrOperator.aopEQ);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrSymbol);
        expr[1].operand1.symbol = new FldSpec(new RelSpec(RelSpec.innerRel),8);
        expr[1].operand2.string = ""+label;

        expr[2] = null;
    }

    public void SortMergeJoin(String filename) {
        AttrType []E1Type = new AttrType[8];
        E1Type[0] = new AttrType(AttrType.attrString);
        E1Type[1] = new AttrType(AttrType.attrInteger);
        E1Type[2] = new AttrType(AttrType.attrInteger);
        E1Type[3] = new AttrType(AttrType.attrInteger);
        E1Type[4] = new AttrType(AttrType.attrInteger);
        E1Type[5] = new AttrType(AttrType.attrInteger);
        E1Type[6] = new AttrType(AttrType.attrString);
        E1Type[7] = new AttrType(AttrType.attrString);

        short []E1size = new short[3];
        E1size[0] = Tuple.LABEL_MAX_LENGTH;
        E1size[1] = 4;
        E1size[2] = 4;

        FldSpec []E1Proj = new FldSpec[8];
        for (int i=0;i<8;i++) {
            E1Proj[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
        }

        AttrType []E2Type = new AttrType[8];
        E2Type[0] = new AttrType(AttrType.attrString);
        E2Type[1] = new AttrType(AttrType.attrInteger);
        E2Type[2] = new AttrType(AttrType.attrInteger);
        E2Type[3] = new AttrType(AttrType.attrInteger);
        E2Type[4] = new AttrType(AttrType.attrInteger);
        E2Type[5] = new AttrType(AttrType.attrInteger);
        E2Type[6] = new AttrType(AttrType.attrString);
        E2Type[7] = new AttrType(AttrType.attrString);

        short []E2size = new short[3];
        E2size[0] = Tuple.LABEL_MAX_LENGTH;
        E2size[1] = 4;
        E2size[2] = 4;

        FldSpec []E2Proj = new FldSpec[8];
        for (int i=0;i<8;i++) {
            E2Proj[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
        }

        FileScan am = null;
        FileScan am2 = null;

        try {
            am = new FileScan(""+filename,E1Type,E1size,(short)8,(short)8,E1Proj,null);
        }
        catch (Exception e) {
            System.err.println(""+e);
        }

        try {
            am2 = new FileScan(""+filename,E2Type,E2size,(short)8,(short)8,E2Proj,null);
        }
        catch (Exception e) {
            System.err.println(""+e);
        }

        FldSpec []projection = new FldSpec[16];
        projection[0] = new FldSpec(new RelSpec(RelSpec.outer),1);
        projection[1] = new FldSpec(new RelSpec(RelSpec.innerRel),1);
        projection[2] = new FldSpec(new RelSpec(RelSpec.outer),3);
        projection[3] = new FldSpec(new RelSpec(RelSpec.outer),4);
        projection[4] = new FldSpec(new RelSpec(RelSpec.outer),5);
        projection[5] = new FldSpec(new RelSpec(RelSpec.outer),6);
        projection[6] = new FldSpec(new RelSpec(RelSpec.outer),7);
        projection[7] = new FldSpec(new RelSpec(RelSpec.outer),8);
        projection[8] = new FldSpec(new RelSpec(RelSpec.outer),2);
        projection[9] = new FldSpec(new RelSpec(RelSpec.innerRel),2);
        projection[10] = new FldSpec(new RelSpec(RelSpec.innerRel),3);
        projection[11] = new FldSpec(new RelSpec(RelSpec.innerRel),4);
        projection[12] = new FldSpec(new RelSpec(RelSpec.innerRel),5);
        projection[13] = new FldSpec(new RelSpec(RelSpec.innerRel),6);
        projection[14] = new FldSpec(new RelSpec(RelSpec.innerRel),7);
        projection[15] = new FldSpec(new RelSpec(RelSpec.innerRel),8);

        TupleOrder asc = new TupleOrder(TupleOrder.Ascending);
        s = null;
        Descriptor temp = new Descriptor();
        temp.set((short)-1,(short)-1,(short)-1,(short)-1,(short)-1);
        try {
            s = new SortMerge(E1Type,8,E1size,E2Type,8,E2size,
                    7, 4,8,4,50,am,am2,
                    false, false,asc,10,temp,OutFilter,projection,16);
        }
        catch (Exception e) {
            System.err.println("ERROR IN SORT MERGE EDGE");
            System.err.println(""+e);
            e.printStackTrace();
        }
    }

    public Tuple get_next() {
        Tuple temp = new Tuple();
        temp = null;
        try {
            temp = s.get_next();
        }
        catch (Exception e) {
            System.err.println(""+e);
            e.printStackTrace();
        }
        return temp;
    }

    public void close() {
        try {
            s.close();
        }
        catch (Exception e) {
            System.err.println(""+e);
            e.printStackTrace();
        }
    }

}

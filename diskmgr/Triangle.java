package diskmgr;

import java.io.IOException;

import global.*;
import heap.*;
import index.IndexScan;
import iterator.*;

/**
 * Created by vamsikrishnag on 4/24/17.
 */
public class Triangle {
    public IndexNLJ inlj;
    public IndexNLJ nlj;
    public IndexScan left;


    public Triangle (String filename, String q)
    {
        TriangleQuery(filename,q);
    }

    public void TriangleQuery(String filename,String q)
    {
        int[] worl = new int[2];

        CondExpr[] mexpr= new CondExpr[4];
        for (int i=0;i<4;i++)
        {
            mexpr[i] = new CondExpr();
        }
        CondExpr[] lexpr= new CondExpr[2];
        for (int i=0;i<2;i++)
        {
            lexpr[i] = new CondExpr();
        }
        CondExpr[] rexpr= new CondExpr[2];
        for (int i=0;i<2;i++)
        {
            rexpr[i] = new CondExpr();
        }
        CondExpr[] outfilter= new CondExpr[2];
        for (int i=0;i<2;i++)
        {
            outfilter[i] = new CondExpr();
        }

        String[] queries = new String[3];
        for (int i=0;i<3;i++)
        {
            queries[i] = new String();
        }
        queries = q.split(";");

        String[] label = new String[3];
        for (int i=0;i<3;i++)
        {
            label[i] = new String();
        }
        label[0] = queries[0].substring(0,2);
        label[1] = queries[1].substring(0,2);
        label[2] = queries[2].substring(0,2);

        if(label[0].equals("EL") && label[1].equals("EL")){
            String[] edge_label = new String[2];
            edge_label[0] = new String();
            edge_label[1] = new String();
            edge_label[0] = queries[0].substring(2);
            edge_label[1] = queries[1].substring(2);
            mexpr = setCondExprLL(edge_label);
            worl[0] = 1;
            worl[1] = 1;
        }
        else if(label[0].equals("EL") && label[1].equals("EW")){
            String edge_label = new String();
            edge_label = queries[0].substring(2);
            int weight = Integer.parseInt(queries[1].substring(2));
            mexpr = setCondExprLW(edge_label, weight);
            worl[0] = 1;
            worl[1] = 6;

        }
        else if(label[0].equals("EW") && label[1].equals("EW")){
            int[] weights = new int[2];
            String label1 = queries[0].substring(2);
            String label2 = queries[1].substring(2);
            weights[0] = Integer.parseInt(label1);
            weights[1] = Integer.parseInt(label2);
            mexpr = setCondExprWW(weights);
            worl[0] = 6;
            worl[1] = 6;

        }
        else if(label[0].equals("EW") && label[1].equals("EL")){
            String edge_label = new String();
            edge_label = queries[1].substring(2);
            int weight = Integer.parseInt(queries[0].substring(2));
            mexpr = setCondExprWL(edge_label,weight);
            worl[0] = 6;
            worl[1] = 1;
        }
        else{
            System.out.println("Queries not correct");
        }
        //s = new SortMergeEdgeJoin(filename,expr);
        lexpr[0] = mexpr[1];
        lexpr[1] =null;
        rexpr[0] = mexpr[2];
        rexpr[1] = null;

        try
        {
            Join1(filename,lexpr,rexpr,outfilter,worl);
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }

        CondExpr[] expr2 = new CondExpr[4];
        for (int i=0;i<4;i++)
        {
            expr2[i] = new CondExpr();
        }

        if(label[2].equals("EW")) {
            int weight = Integer.parseInt(queries[2].substring(2));
            expr2 = setCondExprW(weight);
        }
        else if(label[2].equals("EL")) {
            expr2 = setCondExprL(queries[2].substring(2));
        }
        nlj = Join2(filename,expr2);
        // Check point
    }

    public void Join1 (String filename, CondExpr[] leftexpr, CondExpr[] rightexpr, CondExpr[] outfilter,int[] WorL) throws Exception {


        FldSpec [] projection = new FldSpec[4];
        projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projection[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        projection[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 7);
        projection[3] = new FldSpec(new RelSpec(RelSpec.innerRel), 8);

        TupleOrder asc = new TupleOrder(TupleOrder.Ascending);

        AttrType[] attrType = new AttrType[8];				//Initiating the Index Scan......
        attrType[0] = new AttrType(AttrType.attrString);
        attrType[1] = new AttrType(AttrType.attrInteger);
        attrType[2] = new AttrType(AttrType.attrInteger);
        attrType[3] = new AttrType(AttrType.attrInteger);
        attrType[4] = new AttrType(AttrType.attrInteger);
        attrType[5] = new AttrType(AttrType.attrInteger);
        attrType[6] = new AttrType(AttrType.attrString);
        attrType[7] = new AttrType(AttrType.attrString);

        FldSpec[] projlist = new FldSpec[2];
        RelSpec rel = new RelSpec(RelSpec.outer);

        projlist[1] = new FldSpec(rel, 8);
        projlist[0] = new FldSpec(rel, 7);

        short[] attrSize = new short[3];
        attrSize[0] = Tuple.LABEL_MAX_LENGTH;
        attrSize[1] = 4;
        attrSize[2] = 4;

        AttrType[] leftAttr = new AttrType[2];
        leftAttr[0] = new AttrType(AttrType.attrString);
        leftAttr[1] = new AttrType(AttrType.attrString);

        short[] leftattrSize = new short[3];
        leftattrSize[0] = 4;
        leftattrSize[1] = 4;

        FldSpec[] indexprojlist = new FldSpec[8];
        for (int i=0;i<8;i++)
        {
            indexprojlist[i] = new FldSpec(rel,i+1);
        }


        AttrType[] output = new AttrType[4];
        for (int i=0;i<4;i++)
        {
            output[i] = new AttrType(AttrType.attrString);
        }

        String[] indexName = {"GraphDBEDGELABEL","GraphDBEDGEWEIGHT"};
        if(WorL[0] == 6)
            indexName[0] = "GraphDBEDGEWEIGHT";
        if(WorL[1] == 1)
            indexName[1] = "GraphDBEDGELABEL";

        left = null;

        try {

            left = new IndexScan(new IndexType(IndexType.B_Index), filename, indexName[0], attrType, attrSize, 8, 2, projlist, leftexpr, WorL[0], false);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.inlj = new IndexNLJ(leftAttr,2, leftattrSize, attrType, 8,
                attrSize, 100, left, "GraphDBEDGESRCLABEL", new IndexType(IndexType.B_Index), filename,indexprojlist,
                7, outfilter, rightexpr, projection, 4, 2,null);
    }

    public IndexNLJ Join2 (String filename,CondExpr[] main_expr) {

        AttrType [] E1types = new AttrType[8];
        E1types[0] = new AttrType(AttrType.attrString);
        E1types[1] = new AttrType(AttrType.attrInteger);
        E1types[2] = new AttrType(AttrType.attrInteger);
        E1types[3] = new AttrType(AttrType.attrInteger);
        E1types[4] = new AttrType(AttrType.attrInteger);
        E1types[5] = new AttrType(AttrType.attrInteger);
        E1types[6] = new AttrType(AttrType.attrString);
        E1types[7] = new AttrType(AttrType.attrString);

        short [] E1sizes = new short[3];
        E1sizes[0] = Tuple.LABEL_MAX_LENGTH;
        E1sizes[1] = 4;
        E1sizes[2] = 4;

        CondExpr[] outfilter = new CondExpr[3];
        outfilter[0] = main_expr[0];
        outfilter[1] = main_expr[1];
        outfilter[2] = null;

        CondExpr[] rightexpr = new CondExpr[2];
        rightexpr[0] = main_expr[2];
        rightexpr[1] = null;

        AttrType [] E2types = new AttrType[4];
        for (int i=0;i<4;i++)
        {
            E2types[i] = new AttrType(AttrType.attrString);
        }

        short [] E2sizes = new short[4];
        for (int i=0;i<4;i++)
        {
            E2sizes[i] = 4;
        }

        FldSpec [] proj_list = new FldSpec[3];
        proj_list[0] = new FldSpec(new RelSpec(RelSpec.innerRel), 7);
        proj_list[1] = new FldSpec(new RelSpec(RelSpec.innerRel), 8);
        proj_list[2] = new FldSpec(new RelSpec(RelSpec.outer), 2);

        RelSpec rel = new RelSpec(RelSpec.outer);

        FldSpec[] indexprojlist = new FldSpec[8];
        for (int i=0;i<8;i++)
        {
            indexprojlist[i] = new FldSpec(rel,i+1);
        }

        IndexNLJ inl = null;
        try {
            inl = new IndexNLJ(E2types,4, E2sizes, E1types, 8,
                    E1sizes, 100, this.nlj, "GraphDBEDGESRCLABEL", new IndexType(IndexType.B_Index), filename,indexprojlist,
                    7, outfilter, rightexpr, proj_list, 3, 4,null);
        } catch(Exception e) {
            e.printStackTrace();
        }
        return inl;
    }


    public Sort Sorting (IndexNLJ n) {
        AttrType[] attrType = new AttrType[3];
        for (int i=0;i<3;i++)
        {
            attrType[i] = new AttrType(AttrType.attrString);
        }

        short[] attrSize = new short[3];
        for (int i=0;i<3;i++)
        {
            attrSize[i] = 4;
        }

        TupleOrder order = new TupleOrder(TupleOrder.Ascending);
        Sort sort = null;

        try {
            sort = new Sort(attrType, (short) 3, attrSize, n, 1,order, 3, 100);
        } catch (SortException | IOException e) {
            e.printStackTrace();
        }
        return sort;
    }

    public void DuplicatesRemoval(IndexNLJ n) throws Exception
    {
        Tuple t = new Tuple();
        t=null;

        Heapfile temp = new Heapfile("TraingleHeap");

        AttrType[] type = new AttrType[4];
        for (int i=0;i<4;i++)
        {
            type[i] = new AttrType(AttrType.attrString);
        }

        FldSpec[] projection = new FldSpec[4];
        RelSpec rel = new RelSpec(RelSpec.outer);
        for (int i=0;i<4;i++)
        {
            projection[i] = new FldSpec(rel,i+1);
        }

        short[] strSizes = new short[4];
        strSizes[0]=30;
        strSizes[1]=4;
        strSizes[2]=4;
        strSizes[3]=4;

        while( (t=n.get_next()) != null )
        {
            String[] values = new String[3];
            values = t.convertArray();
            while(values[0].compareTo(values[1])>0 || values[0].compareTo(values[2])>0)
            {
                String temp1;
                temp1 = values[1];
                values[1] = values[0];
                values[0] = values[2];
                values[2] = temp1;
            }

            Tuple t1= new Tuple();
            t1.setHdr((short)4, type, strSizes);
            String concat = values[0]+values[1]+values[2];
            t1.setStrFld(1, concat);
            t1.setStrFld(2, values[0]);
            t1.setStrFld(3, values[1]);
            t1.setStrFld(4, values[2]);

            temp.insertRecord(t1.getTupleByteArray());

        }

        FileScan f = new FileScan("Triangleheap", type, strSizes, (short) 4, 4, projection, null);
        DuplElim dup = new DuplElim(type,(short)4,strSizes,f, 100, false);
        Tuple t1 = new Tuple();
        int[] fldno = {2,3,4};
        while((t1=dup.get_next())!=null) {
            t1.print(type,fldno);
        }
        dup.close();
        temp.deleteFile();

    }

    private static CondExpr[] setCondExprLL(String[] labels)
    {

        CondExpr[] expr= new CondExpr[4];
        for (int i=0;i<4;i++)
        {
            expr[i] = new CondExpr();
        }

        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),8);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),7);

        expr[1].next   = null;
        expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrString);
        expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
        expr[1].operand2.string = labels[0];

        expr[2].next   = null;
        expr[2].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[2].type1 = new AttrType(AttrType.attrSymbol);
        expr[2].type2 = new AttrType(AttrType.attrString);
        expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
        expr[2].operand2.string = labels[1];

        expr[3] = null;

        return expr;
    }

    private static CondExpr[] setCondExprWW(int[] weights)
    {
        CondExpr[] expr= new CondExpr[4];
        for (int i=0;i<4;i++)
        {
            expr[i] = new CondExpr();
        }

        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),8);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),7);

        expr[1].next   = null;
        expr[1].op    = new AttrOperator(AttrOperator.aopLE);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrInteger);
        expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),6);
        expr[1].operand2.integer = weights[0];

        expr[2].next   = null;
        expr[2].op    = new AttrOperator(AttrOperator.aopLE);
        expr[2].type1 = new AttrType(AttrType.attrSymbol);
        expr[2].type2 = new AttrType(AttrType.attrInteger);
        expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),6);
        expr[2].operand2.integer = weights[1];

        expr[3] = null;

        return expr;
    }

    private static CondExpr[] setCondExprLW(String label,int weight)
    {
        CondExpr[] expr= new CondExpr[4];
        for (int i=0;i<4;i++)
        {
            expr[i] = new CondExpr();
        }

        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),8);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),7);

        expr[1].next   = null;
        expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrString);
        expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),1);
        expr[1].operand2.string = label;

        expr[2].next   = null;
        expr[2].op    = new AttrOperator(AttrOperator.aopLE);
        expr[2].type1 = new AttrType(AttrType.attrSymbol);
        expr[2].type2 = new AttrType(AttrType.attrInteger);
        expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),6);
        expr[2].operand2.integer = weight;

        expr[3] = null;

        return expr;
    }

    private static CondExpr[] setCondExprWL(String label,int weight)
    {
        CondExpr[] expr= new CondExpr[4];
        for (int i=0;i<4;i++)
        {
            expr[i] = new CondExpr();
        }

        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),8);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),7);

        expr[1].next   = null;
        expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrString);
        expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
        expr[1].operand2.string = label;

        expr[2].next   = null;
        expr[2].op    = new AttrOperator(AttrOperator.aopLE);
        expr[2].type1 = new AttrType(AttrType.attrSymbol);
        expr[2].type2 = new AttrType(AttrType.attrInteger);
        expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),6);
        expr[2].operand2.integer = weight;

        expr[3] = null;

        return expr;
    }
    private static CondExpr[] setCondExprW(int weight)
    {
        CondExpr[] expr = new CondExpr[4];
        for (int i=0;i<4;i++)
        {
            expr[i] = new CondExpr();
        }

        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),7);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);

        expr[1].next   = null;
        expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrSymbol);
        expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),8);
        expr[1].operand2.symbol =  new FldSpec (new RelSpec(RelSpec.outer),1);

        expr[2].next   = null;
        expr[2].op    = new AttrOperator(AttrOperator.aopLE);
        expr[2].type1 = new AttrType(AttrType.attrSymbol);
        expr[2].type2 = new AttrType(AttrType.attrInteger);
        expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),6);
        expr[2].operand2.integer = weight;

        expr[3] = null;

        return expr;
    }

    private static CondExpr[] setCondExprL(String label)
    {
        CondExpr[] expr = new CondExpr[4];
        for (int i=0;i<4;i++)
        {
            expr[i] = new CondExpr();
        }

        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),7);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);

        expr[1].next   = null;
        expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrSymbol);
        expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),8);
        expr[1].operand2.symbol =  new FldSpec (new RelSpec(RelSpec.outer),1);

        expr[2].next   = null;
        expr[2].op    = new AttrOperator(AttrOperator.aopLE);
        expr[2].type1 = new AttrType(AttrType.attrSymbol);
        expr[2].type2 = new AttrType(AttrType.attrString);
        expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),6);
        expr[2].operand2.string = label;

        expr[3] = null;

        return expr;
    }
}

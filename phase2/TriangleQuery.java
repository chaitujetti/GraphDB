package phase2;

import global.*;
import iterator.*;
import heap.*;

import java.io.IOException;

/**
 * Created by vamsikrishnag on 4/25/17.
 */
public class TriangleQuery implements GlobalConst{
    FileScan iter1,iter2, iter3;
    CondExpr[] expr1,expr2;
    AttrType[] E1Type,E2Type,E3Type,otype;
    FldSpec[] projection1,projection2;
    SortMerge s1,s2;

    public TriangleQuery(SystemDefs systemdef,String query) throws Exception {
        //Extract condition expressions for queries
        String[] queries;
        queries = query.split(";");
        String[][] label = new String[3][2];
        label[0] = queries[0].split(":");
        label[1] = queries[1].split(":");
        label[2] = queries[2].split(":");
        if(label[0][0].equals("L") && label[1][0].equals("L")){
            String[] edge_label = new String[2];
            edge_label[0] = label[0][1];
            edge_label[1] = label[1][1];
            expr1 = setCondExprLL(edge_label);
        }
        else if(label[0][0].equals("L") && label[1][0].equals("W")){
            expr1 = setCondExprLW(label[0][1], Integer.parseInt(label[1][1]));
        }
        else if(label[0][0].equals("W") && label[1][0].equals("W")){
            int[] weights = new int[2];
            String label1 = label[0][1];
            String label2 = label[1][1];
            weights[0] = Integer.parseInt(label1);
            weights[1] = Integer.parseInt(label2);
            expr1 = setCondExprWW(weights);
        }
        else if(label[0][0].equals("W") && label[1][0].equals("L")){
            expr1 = setCondExprWL(label[1][1], Integer.parseInt(label[0][1]));
        }
        else{
            System.out.println("Queries not correct");
        }
        if(label[2][0].equals("L")){
            expr2 = setCondExprL(label[2][1]);
        }
        else{
            expr2 = setCondExprW(Integer.parseInt(label[2][1]));
        }

        E1Type = new AttrType[8];
        E1Type[0] = new AttrType(AttrType.attrString);
        E1Type[1] = new AttrType(AttrType.attrString);
        E1Type[2] = new AttrType(AttrType.attrString);
        E1Type[3] = new AttrType(AttrType.attrInteger);
        E1Type[4] = new AttrType(AttrType.attrInteger);
        E1Type[5] = new AttrType(AttrType.attrInteger);
        E1Type[6] = new AttrType(AttrType.attrInteger);
        E1Type[7] = new AttrType(AttrType.attrInteger);

        short []E1size = new short[3];
        E1size[0] = Tuple.LABEL_MAX_LENGTH;
        E1size[1] = Tuple.LABEL_MAX_LENGTH;
        E1size[2] = Tuple.LABEL_MAX_LENGTH;


        FldSpec []E1Proj = new FldSpec[8];
        for (int i=0;i<8;i++) {
            E1Proj[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
        }

        FldSpec []E2Proj = new FldSpec[8];
        for (int i=0;i<8;i++) {
            E2Proj[i] = new FldSpec(new RelSpec(RelSpec.innerRel),i+1);
        }

        E3Type = new AttrType[8];
        E3Type[0] = new AttrType(AttrType.attrString);
        E3Type[1] = new AttrType(AttrType.attrString);
        E3Type[2] = new AttrType(AttrType.attrString);
        E3Type[3] = new AttrType(AttrType.attrInteger);
        E3Type[4] = new AttrType(AttrType.attrString);
        E3Type[5] = new AttrType(AttrType.attrString);
        E3Type[6] = new AttrType(AttrType.attrString);
        E3Type[7] = new AttrType(AttrType.attrInteger);

        short []E3size = new short[6];
        E3size[0] = Tuple.LABEL_MAX_LENGTH;
        E3size[1] = Tuple.LABEL_MAX_LENGTH;
        E3size[2] = Tuple.LABEL_MAX_LENGTH;
        E3size[3] = Tuple.LABEL_MAX_LENGTH;
        E3size[4] = Tuple.LABEL_MAX_LENGTH;
        E3size[5] = Tuple.LABEL_MAX_LENGTH;

        iter1 = new FileScan(systemdef.JavabaseDB.getEhf()._fileName,E1Type,E1size,(short)8,(short)8,E1Proj,null);
        iter2 = new FileScan(systemdef.JavabaseDB.getEhf()._fileName,E1Type,E1size,(short)8,(short)8,E1Proj,null);
        iter3 = new FileScan(systemdef.JavabaseDB.getEhf()._fileName,E1Type,E1size,(short)8,(short)8,E1Proj,null);

        TupleOrder asc = new TupleOrder(TupleOrder.Ascending);
        Sort sort1 = new Sort(E1Type,(short)8,E1size,iter1,1,asc,10,100);
        //Sort sort2 = new Sort(E1Type,(short)8,E1size,iter2,2,asc,10,10);
        //Sort sort3 = new Sort(E1Type,(short)8,E1size,iter3,2,asc,10,10);

        if( sort1.get_next()== null) {
            System.out.println("sort1 is null");
        }
        projection1 = new FldSpec[8];
        projection1[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projection1[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        projection1[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        projection1[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
        projection1[4] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        projection1[5] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
        projection1[6] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
        projection1[7] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);

        projection2 = new FldSpec[12];
        projection2[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projection2[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        projection2[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        projection2[3] = new FldSpec(new RelSpec(RelSpec.outer), 4);
        projection2[4] = new FldSpec(new RelSpec(RelSpec.outer), 5);
        projection2[5] = new FldSpec(new RelSpec(RelSpec.outer), 6);
        projection2[6] = new FldSpec(new RelSpec(RelSpec.outer), 7);
        projection2[7] = new FldSpec(new RelSpec(RelSpec.outer), 8);
        projection2[8] = new FldSpec(new RelSpec(RelSpec.innerRel), 1);
        projection2[9] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
        projection2[10] = new FldSpec(new RelSpec(RelSpec.innerRel), 3);
        projection2[11] = new FldSpec(new RelSpec(RelSpec.innerRel), 4);

        //TupleOrder asc = new TupleOrder(TupleOrder.Ascending);
        s1 = new SortMerge(E1Type,8,E1size,E1Type,8,E1size,
                2, 10,1,10,50,iter1,iter2,
                false, false,asc,expr1,projection1,8);

        s2 = new SortMerge(E3Type,8,E3size,E1Type,8,E1size,
                6, 10,1,10,50,s1,iter3,
                false, false,asc,expr2,projection2,12);

        if( s1 == null){
            System.out.println("s1 is null");
        }
        if( s2 == null){
            System.out.println("s2 is null");
        }
        otype = new AttrType[12];
        otype[0] = new AttrType(AttrType.attrString);
        otype[1] = new AttrType(AttrType.attrString);
        otype[2] = new AttrType(AttrType.attrString);
        otype[3] = new AttrType(AttrType.attrInteger);
        otype[4] = new AttrType(AttrType.attrString);
        otype[5] = new AttrType(AttrType.attrString);
        otype[6] = new AttrType(AttrType.attrString);
        otype[7] = new AttrType(AttrType.attrInteger);
        otype[8] = new AttrType(AttrType.attrString);
        otype[9] = new AttrType(AttrType.attrString);
        otype[10] = new AttrType(AttrType.attrString);
        otype[11] = new AttrType(AttrType.attrInteger);

        Tuple temp= s1.get_next();
        /* while(temp != null){
            temp.print(otype);
        }*/
        if (temp == null){
            System.out.println("temp is null");
        }
    }

    private static CondExpr[] setCondExprLL(String[] labels)
    {
        CondExpr[] expr= new CondExpr[4];
        expr[3] = null;

        expr[0] = new CondExpr();
        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

        expr[1] = new CondExpr();
        expr[1].next   = null;
        expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrString);
        expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
        expr[1].operand2.string = labels[0];

        expr[2] = new CondExpr();
        expr[2].next   = null;
        expr[2].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[2].type1 = new AttrType(AttrType.attrSymbol);
        expr[2].type2 = new AttrType(AttrType.attrString);
        expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
        expr[2].operand2.string = labels[1];

        return expr;
    }

    private static CondExpr[] setCondExprWW(int[] weights)
    {
        CondExpr[] expr= new CondExpr[4];
        expr[3] = null;

        expr[0] = new CondExpr();
        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

        expr[1] = new CondExpr();
        expr[1].next   = null;
        expr[1].op    = new AttrOperator(AttrOperator.aopLE);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrInteger);
        expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
        expr[1].operand2.integer = weights[0];

        expr[2] = new CondExpr();
        expr[2].next   = null;
        expr[2].op    = new AttrOperator(AttrOperator.aopLE);
        expr[2].type1 = new AttrType(AttrType.attrSymbol);
        expr[2].type2 = new AttrType(AttrType.attrInteger);
        expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),4);
        expr[2].operand2.integer = weights[1];

        return expr;
    }

    private static CondExpr[] setCondExprLW(String label,int weight)
    {
        CondExpr[] expr= new CondExpr[4];
        expr[3] = null;

        expr[0] = new CondExpr();
        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

        expr[1] = new CondExpr();
        expr[1].next   = null;
        expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrString);
        expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
        expr[1].operand2.string = label;

        expr[2] = new CondExpr();
        expr[2].next   = null;
        expr[2].op    = new AttrOperator(AttrOperator.aopLE);
        expr[2].type1 = new AttrType(AttrType.attrSymbol);
        expr[2].type2 = new AttrType(AttrType.attrInteger);
        expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),4);
        expr[2].operand2.integer = weight;

        return expr;
    }

    private static CondExpr[] setCondExprWL(String label,int weight)
    {
        CondExpr[] expr= new CondExpr[4];
        expr[3] = null;

        expr[0] = new CondExpr();
        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

        expr[1] = new CondExpr();
        expr[1].next   = null;
        expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrString);
        expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
        expr[1].operand2.string = label;

        expr[2] = new CondExpr();
        expr[2].next   = null;
        expr[2].op    = new AttrOperator(AttrOperator.aopLE);
        expr[2].type1 = new AttrType(AttrType.attrSymbol);
        expr[2].type2 = new AttrType(AttrType.attrInteger);
        expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
        expr[2].operand2.integer = weight;

        return expr;
    }
    private static CondExpr[] setCondExprW(int weight)
    {
        CondExpr[] expr= new CondExpr[3];
        expr[2] = null;

        expr[0] = new CondExpr();
        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.outer),6);

        expr[1] = new CondExpr();
        expr[1].next   = null;
        expr[1].op    = new AttrOperator(AttrOperator.aopLE);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrInteger);
        expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),4);
        expr[1].operand2.integer = weight;

        return expr;
    }

    private static CondExpr[] setCondExprL(String label)
    {
        CondExpr[] expr= new CondExpr[3];
        expr[2] = null;

        expr[0] = new CondExpr();
        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.outer),6);

        expr[1] = new CondExpr();
        expr[1].next   = null;
        expr[1].op    = new AttrOperator(AttrOperator.aopLE);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrString);
        expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
        expr[1].operand2.string = label;

        return expr;
    }

}

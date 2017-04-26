package phase2;

import global.*;
import iterator.*;
import heap.*;

import java.io.IOException;

/**
 * Created by vamsikrishnag on 4/25/17.
 */
public class TriangleQuery implements GlobalConst{
    FileScan iter1,iter2, iter3, iter4;
    CondExpr[] expr1,expr2, exprf1, exprf2, exprf3;
    AttrType[] attr1,attr2,otype;
    FldSpec[] projection1,projection2;
    SortMerge s1,s2;

    public TriangleQuery(SystemDefs systemdef,String query) throws Exception {
        String[] queries = query.split(";");
        expr1 = condExpr1(queries[0], queries[1]);
        expr2 = condExpr2(queries[2]);
        exprf1 = new CondExpr[2];
        exprf1[0] = expr1[1];
        exprf1[1] = null;
        exprf2 = new CondExpr[2];
        exprf2[0] = expr1[2];
        exprf2[1] = null;
        exprf3 = new CondExpr[2];
        exprf3[0] = expr2[2];
        exprf3[1] = null;

        attr1 = new AttrType[8];
        attr1[0] = new AttrType(AttrType.attrString);
        attr1[1] = new AttrType(AttrType.attrString);
        attr1[2] = new AttrType(AttrType.attrString);
        attr1[3] = new AttrType(AttrType.attrInteger);
        attr1[4] = new AttrType(AttrType.attrInteger);
        attr1[5] = new AttrType(AttrType.attrInteger);
        attr1[6] = new AttrType(AttrType.attrInteger);
        attr1[7] = new AttrType(AttrType.attrInteger);

        short[] size1 = new short[3];
        size1[0] = Tuple.LABEL_MAX_LENGTH;
        size1[1] = Tuple.LABEL_MAX_LENGTH;
        size1[2] = Tuple.LABEL_MAX_LENGTH;


        FldSpec []proj1 = new FldSpec[8];
        for (int i=0;i<8;i++) {
            proj1[i] = new FldSpec(new RelSpec(RelSpec.outer),i+1);
        }

        attr2 = new AttrType[3];
        attr2[0] = new AttrType(AttrType.attrString);//s1
        attr2[1] = new AttrType(AttrType.attrString);//d1 or s2
        attr2[2] = new AttrType(AttrType.attrString);//d2
        
        short[] size2 = new short[3];
        size2[0] = Tuple.LABEL_MAX_LENGTH;
        size2[1] = Tuple.LABEL_MAX_LENGTH;
        size2[2] = Tuple.LABEL_MAX_LENGTH;

        otype = new AttrType[3];
        otype[0] = new AttrType(AttrType.attrString);
        otype[1] = new AttrType(AttrType.attrString);
        otype[2] = new AttrType(AttrType.attrString);

        iter1 = new FileScan(systemdef.JavabaseDB.getEhf()._fileName,attr1,size1,(short)8,(short)8,proj1,null);
        iter2 = new FileScan(systemdef.JavabaseDB.getEhf()._fileName,attr1,size1,(short)8,(short)8,proj1,null);
        iter3 = new FileScan(systemdef.JavabaseDB.getEhf()._fileName,attr1,size1,(short)8,(short)8,proj1,null);

        TupleOrder asc = new TupleOrder(TupleOrder.Ascending);
        
        projection1 = new FldSpec[3];
        projection1[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projection1[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        projection1[2] = new FldSpec(new RelSpec(RelSpec.innerRel), 2);
        
        projection2 = new FldSpec[3];
        projection2[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);
        projection2[1] = new FldSpec(new RelSpec(RelSpec.outer), 2);
        projection2[2] = new FldSpec(new RelSpec(RelSpec.outer), 3);
        
        s1 = new SortMerge(attr1,8,size1,attr1,8,size1,2,10,1,10,512,iter1,iter2,false,false,asc,expr1,projection1,3);

        Heapfile hf=new Heapfile("HeapFile_"+systemdef.JavabaseDB.DBname);
        Tuple tem = new Tuple();
        tem.setHdr((short)3, attr2, size2);
        tem = s1.get_next();
        while(tem != null){
            hf.insertRecord(tem.getTupleByteArray());
            tem = s1.get_next();
        }
        iter1.close();
        iter2.close();
        s1.close();

        iter4 = new FileScan(hf._fileName,attr2,size2,(short)3,(short)3,projection2,null);

        s2 = new SortMerge(attr2,3,size2,attr1,8,size1,3,10,1,10,1024,iter4,iter3,false,false,asc,expr2,projection2,3);

        Tuple temp= s2.get_next();
        if (temp == null){
            System.out.println("temp is null");
        }
        while(temp != null){
            temp.print(otype);
            temp = s2.get_next();
        }

        iter4.close();
        iter3.close();
        s2.close();
    }

    private static CondExpr[] condExpr1(String label1,String label2) {
        CondExpr[] expr= new CondExpr[4];
        expr[3] = null;

        expr[0] = new CondExpr();
        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

        String[] values = label1.split(":");
        expr[1] = new CondExpr();
        expr[1].next   = null;
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        if(values[0].equals("L")){
            expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
            expr[1].type2 = new AttrType(AttrType.attrString);
            expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
            expr[1].operand2.string = values[1];
        } else {
            expr[1].op    = new AttrOperator(AttrOperator.aopLE);
            expr[1].type2 = new AttrType(AttrType.attrInteger);
            expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
            expr[1].operand2.integer = Integer.parseInt(values[1]);
        }
        
        values = label2.split(":");
        expr[2] = new CondExpr();
        expr[2].next   = null;
        expr[2].type1 = new AttrType(AttrType.attrSymbol);
        if(values[0].equals("L")){
            expr[2].op    = new AttrOperator(AttrOperator.aopEQ);
            expr[2].type2 = new AttrType(AttrType.attrString);
            expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
            expr[2].operand2.string = values[1];
        } else {
            expr[2].op    = new AttrOperator(AttrOperator.aopLE);
            expr[2].type2 = new AttrType(AttrType.attrInteger);
            expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),4);
            expr[2].operand2.integer = Integer.parseInt(values[1]);
        }

        return expr;
    }

    private static CondExpr[] condExpr2(String label){
        CondExpr[] expr = new CondExpr[4];
        expr[3] = null;
  
        expr[0] = new CondExpr();
        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
  
        expr[1] = new CondExpr();
        expr[1].next   = null;
        expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrSymbol);
        expr[1].operand2.symbol =  new FldSpec (new RelSpec(RelSpec.outer),1);
        expr[1].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);
  
        String[] values = label.split(":");
        expr[2] = new CondExpr();
        expr[2].next   = null;
        expr[2].type1 = new AttrType(AttrType.attrSymbol);
        if(values[0].equals("L")){
            expr[2].op    = new AttrOperator(AttrOperator.aopEQ);
            expr[2].type2 = new AttrType(AttrType.attrString);
            expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),3);
            expr[2].operand2.string = values[1];
        } else {
            expr[2].op    = new AttrOperator(AttrOperator.aopLE);
            expr[2].type2 = new AttrType(AttrType.attrInteger);
            expr[2].operand1.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),4);
            expr[2].operand2.integer = Integer.parseInt(values[1]);
        }

        return expr;
    }

}

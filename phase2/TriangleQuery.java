package phase2;

import global.*;
import iterator.*;
import heap.*;
import diskmgr.*;
import bufmgr.*;
import btree.*;

import java.io.IOException;

/**
 * Created by vamsikrishnag on 4/25/17.
 */
public class TriangleQuery implements GlobalConst{
    
    Heapfile results;

    public TriangleQuery(SystemDefs systemdef,String query) throws Exception {
        this.results = new Heapfile("ResultHeapFile_"+systemdef.JavabaseDB.DBname);

        FileScan iter1,iter2, iter3;
        CondExpr[] expr1,expr2, exprf1, exprf2, exprf3;
        AttrType[] attr1,attr2, attr3, otype;
        FldSpec[] projection1,projection2;
        SortMerge s1,s2;

        String[] queries = query.split(";");
        expr1 = condExpr1(queries[0], queries[1]);
        expr2 = condExpr2(queries[2]);

        exprf1 = new CondExpr[2];
        exprf1[0] = new CondExpr();
        exprf1[0] = expr1[1];
        exprf1[1] = null;

        exprf2 = new CondExpr[2];
        exprf2[0] = new CondExpr();
        String[] values = queries[1].split(":");
        exprf2[0] = new CondExpr();
        exprf2[0].next   = null;
        exprf2[0].type1 = new AttrType(AttrType.attrSymbol);
        if(values[0].equals("L")){
            exprf2[0].op    = new AttrOperator(AttrOperator.aopEQ);
            exprf2[0].type2 = new AttrType(AttrType.attrString);
            exprf2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
            exprf2[0].operand2.string = values[1];
        } else {
            exprf2[0].op    = new AttrOperator(AttrOperator.aopLE);
            exprf2[0].type2 = new AttrType(AttrType.attrInteger);
            exprf2[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
            exprf2[0].operand2.integer = Integer.parseInt(values[1]);
        }
        exprf2[1] = null;

        exprf3 = new CondExpr[2];
        exprf3[0] = new CondExpr();
        values = queries[2].split(":");
        exprf3[0] = new CondExpr();
        exprf3[0].next   = null;
        exprf3[0].type1 = new AttrType(AttrType.attrSymbol);
        if(values[0].equals("L")){
            exprf3[0].op    = new AttrOperator(AttrOperator.aopEQ);
            exprf3[0].type2 = new AttrType(AttrType.attrString);
            exprf3[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
            exprf3[0].operand2.string = values[1];
        } else {
            exprf3[0].op    = new AttrOperator(AttrOperator.aopLE);
            exprf3[0].type2 = new AttrType(AttrType.attrInteger);
            exprf3[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),4);
            exprf3[0].operand2.integer = Integer.parseInt(values[1]);
        }
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

        iter1 = new FileScan(systemdef.JavabaseDB.getEhf()._fileName,attr1,size1,(short)8,(short)8,proj1,exprf1);
        iter2 = new FileScan(systemdef.JavabaseDB.getEhf()._fileName,attr1,size1,(short)8,(short)8,proj1,exprf2);

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
        tem = s1.get_next();
        tem.setHdr((short)3, attr2, size2);
        if (tem == null){
            System.out.println("tem is null");
        }
        while(tem != null){
            // tem.print(otype);
            hf.insertRecord(tem.getTupleByteArray());
            tem.setHdr((short)3, attr2, size2);
            tem = s1.get_next();
        }

        iter1.close();
        iter2.close();
        try{
            s1.close();
        } catch(Exception e){
            System.out.println("Sort Done, Closing Failed");
        }
        
        // iter3 = new FileScan(systemdef.JavabaseDB.getEhf()._fileName,attr1,size1,(short)8,(short)8,proj1,exprf3);
        iter3 = new FileScan(hf._fileName,attr2,size2,(short)3,(short)3,projection2,null);

        // s2 = new SortMerge(attr2,3,size2,attr1,8,size1,3,10,1,10,512,iter3,iter3,false,false,asc,expr2,projection2,3);

        // Tuple temp = s2.get_next();
        // if (temp == null){
        //     System.out.println("temp is null");
        // }
        // while(temp != null){
        //     temp.print(otype);
        //     temp = s2.get_next();
        // }

        // 
        // // iter3.close();
        // try{
        //     s2.close();
        // } catch(Exception e){
        //     System.out.println("Sort Done, Closing Failed");
        // }

        Tuple tuple = new Tuple();

        attr3 = new AttrType[1];
        attr3[0] = new AttrType(AttrType.attrString);

        short[] size3 = new short[3];
        size3[0] = 3 * Tuple.LABEL_MAX_LENGTH + 2;


        String des, merged;
        BTFileScan scan = null;
        KeyDataEntry entry;
        Tuple temp = iter3.get_next();
        EID edgeId = new EID();
        if (temp == null){
            System.out.println("temp is null");
        }
        while(temp != null){
            des = temp.getStrFld(3);
            scan = systemdef.JavabaseDB.getEdgeSourceIndex().new_scan(new StringKey(des), new StringKey(des));
            entry = scan.get_next();
            while(entry != null) {
                LeafData leafNode=(LeafData)entry.data;
                RID record = leafNode.getData();
                edgeId.pageNo.pid = record.pageNo.pid;
                edgeId.slotNo = record.slotNo;
                Edge edge=systemdef.JavabaseDB.getEhf().getEdge(edgeId);
                if(edge.getDestinationLabel().equals(temp.getStrFld(1))){
                    if(values[0].equals("L")){
                        if(edge.getLabel().equals(values[1])){
                            // temp.print(otype);
                            merged = temp.getStrFld(1) + "_" + temp.getStrFld(2) + "_" + temp.getStrFld(3);
                            tuple.setHdr((short)1, attr3, size3);
                            tuple.setStrFld(1, merged);
                            this.results.insertRecord(tuple.getTupleByteArray());
                        }
                    }
                    if(values[0].equals("W")){
                        if(edge.getWeight() <= Integer.parseInt(values[1])){
                            // temp.print(otype);
                            merged = temp.getStrFld(1) + "_" + temp.getStrFld(2) + "_" + temp.getStrFld(3);
                            tuple.setHdr((short)1, attr3, size3);
                            tuple.setStrFld(1, merged);
                            this.results.insertRecord(tuple.getTupleByteArray());
                        }
                    }
                }
                entry = scan.get_next();
            }
            temp = iter3.get_next();
        }

        iter3.close();
        scan.DestroyBTreeFileScan();
        hf.deleteFile();
    }

    private static CondExpr[] condExpr1(String label1,String label2) {
        CondExpr[] expr= new CondExpr[2];
        expr[1] = null;

        expr[0] = new CondExpr();
        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),2);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);

        return expr;
    }

    private static CondExpr[] condExpr2(String label){
        CondExpr[] expr = new CondExpr[3];
        expr[2] = null;
  
        expr[0] = new CondExpr();
        expr[0].next  = null;
        expr[0].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[0].type1 = new AttrType(AttrType.attrSymbol);
        expr[0].type2 = new AttrType(AttrType.attrSymbol);
        expr[0].operand1.symbol = new FldSpec (new RelSpec(RelSpec.outer),3);
        expr[0].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),1);
  
        expr[1] = new CondExpr();
        expr[1].next   = null;
        expr[1].op    = new AttrOperator(AttrOperator.aopEQ);
        expr[1].type1 = new AttrType(AttrType.attrSymbol);
        expr[1].type2 = new AttrType(AttrType.attrSymbol);
        expr[1].operand1.symbol =  new FldSpec (new RelSpec(RelSpec.outer),1);
        expr[1].operand2.symbol = new FldSpec (new RelSpec(RelSpec.innerRel),2);

        return expr;
    }

    public void print(String type) throws Exception {
        AttrType[] attr = new AttrType[1];
        attr[0] = new AttrType(AttrType.attrString);

        short[] size = new short[3];
        size[0] = 3 * Tuple.LABEL_MAX_LENGTH + 2;

        FldSpec[] projection = new FldSpec[1];
        projection[0] = new FldSpec(new RelSpec(RelSpec.outer), 1);

        TupleOrder asc = new TupleOrder(TupleOrder.Ascending);

        FileScan iter = new FileScan(this.results._fileName,attr,size,(short)1,(short)1,projection,null);
        if(type.equals("a")){
            Tuple t = new Tuple();
            t = iter.get_next();
            while(t!=null){
                t.print(attr);
                t = iter.get_next();
            }
            iter.close();
            return;
        } else {
            Sort newSort = new Sort(attr, (short)1, size, iter, 1, asc, size[0], 10);
            if(type.equals("b")){
                Tuple t = new Tuple();
                t = newSort.get_next();
                while(t!=null){
                    t.print(attr);
                    t = newSort.get_next();
                }
                
                try{
                    iter.close();
                    newSort.close();
                }catch(Exception e){
                    System.out.println("Sorting Done, Close Failed");
                }
                return;
            } else {
                Tuple t = new Tuple();
                String tstr = null;
                String lstr = null;
                t = newSort.get_next();
                tstr = t.getStrFld(1);
                while(t!=null){
                    if(!tstr.equals(lstr)){
                        t.print(attr);
                        lstr = tstr;
                    }
                    t = newSort.get_next();
                    if(t!=null){
                        tstr = t.getStrFld(1);
                    }
                }
                
                try{
                    iter.close();
                    newSort.close();
                }catch(Exception e){
                    System.out.println("Sorting Done, Close Failed");
                }
                return;
            }
        }
    }

    public void close() throws Exception {
        this.results.deleteFile();
    }
}

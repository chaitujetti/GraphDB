package phase2;

import bufmgr.PagePinnedException;
import diskmgr.DiskMgrException;
import diskmgr.FileIOException;
import diskmgr.InvalidPageNumberException;
import global.GlobalConst;
import global.SystemDefs;
import heap.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Scanner;
import java.io.File;
import java.io.FileFilter;
import query.*;

/**
 * Created by joelmascarenhas on 3/11/17.
 */

class GraphDriver {
    private void menu() {
        System.out.println("-------------------------- MENU ------------------");
        System.out.println("\n\n[1] Task 10 - Batch Node Insert");
        System.out.println("[2] Task 11 - Batch Edge Insert");
        System.out.println("[3] Task 12 - Batch Node Delete");
        System.out.println("[4] Task 13 - Batch Edge Delete");
        System.out.println("[5] Task 14 - Simple Node Query");
        System.out.println("[6] Task 15 - Simple Edge Query");
        System.out.println("[7] Path Expression Query 1");
        System.out.println("[8] Path Expression Query 2");
        System.out.println("[9] Path Expression Query 3");
        System.out.println("\n\n[10]  Quit!");
        System.out.print("Hi, make your choice :");
    }

    private int dbExists(String dbFile){
        File[] fileList = new File(".").listFiles(new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().equalsIgnoreCase(dbFile);
            }
        });
        return fileList.length;
    }

    public void runTests() throws Exception {
        int choice = -1;
        Scanner scanner = new Scanner(System.in);
        boolean status = false;
        menu();
        boolean dbcreated=false;
        SystemDefs systemdef=null;

        while (choice != 10) {
            menu();

            //try {
            choice = GetStuff.getChoice();
            String inp;
            String[] varargs;
            switch (choice) {
                case 1:

                    //Task 2.10 Batch Node Insert
                    System.out.print(" Enter NODEFILENAME GRAPHDBNAME :-");
                    inp = scanner.nextLine();
                    varargs = inp.split(" ");

                    if (dbExists(varargs[1]) ==1){
                        dbcreated=true;//check if DB file already exists
                    }
                    else dbcreated=false;
                    if(dbcreated!=true)//db doesnt exist
                    {
                        systemdef=new SystemDefs(varargs[1],1000,256,"Clock");
                        systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        //dbcreated=true;
                    }
                    else if (dbcreated==true && (systemdef==null)) {//db file exists,minibase restarted
                        SystemDefs.MINIBASE_RESTART_FLAG=true;
                        systemdef=new SystemDefs(varargs[1],1000,256,"Clock");
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                    }
                    status = batchnodeinsert.nodeinsert(varargs[0],varargs[1],systemdef);


                    if(status == true ) {
                        System.out.println("Batch Node Insert successful");

                        //systemdef.JavabaseDB.closeDB();
                    }
                    else
                        System.out.println("Batch Node Insert failed");
                    break;
                case 2:
                    //Task 2.11 Batch Edge Insert
                    System.out.print(" Enter EDGEFILENAME GRAPHDBNAME :-");
                    inp = scanner.nextLine();
                    varargs = inp.split(" ");
                    if (dbExists(varargs[1]) ==1){
                        dbcreated=true;//check if DB file already exists
                    }
                    else dbcreated=false;
                    if(dbcreated!=true)//db doesnt exist
                    {
                        systemdef=new SystemDefs(varargs[1],1000,256,"Clock");
                        systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        //dbcreated=true;
                    }
                    else if (dbcreated==true && (systemdef==null)) {//db file exists,minibase restarted
                        SystemDefs.MINIBASE_RESTART_FLAG=true;
                        systemdef=new SystemDefs(varargs[1],1000,256,"Clock");
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                    }
                    status = batchedgeinsert.edgeinsert(varargs[0],varargs[1],systemdef);

                    if(status == true )
                        System.out.println("Batch Edge Insert successful");
                    else
                        System.out.println("Batch Edge Insert failed");
                    break;
                case 3:
                    //Task 2.12 Batch Node Delete
                    System.out.print(" Enter NODEFILENAME GRAPHDBNAME :-");
                    inp = scanner.nextLine();
                    varargs = inp.split(" ");
                    if (dbExists(varargs[1]) ==1){
                        dbcreated=true;//check if DB file already exists
                    }
                    else dbcreated=false;
                    if(dbcreated!=true)//db doesnt exist
                    {
                        systemdef=new SystemDefs(varargs[1],1000,256,"Clock");
                        systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        //dbcreated=true;
                    }
                    else if (dbcreated==true && (systemdef==null)) {//db file exists,minibase restarted
                        SystemDefs.MINIBASE_RESTART_FLAG=true;
                        systemdef=new SystemDefs(varargs[1],1000,256,"Clock");
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                    }
                    status = batchnodedelete.nodedelete(varargs[0],varargs[1],systemdef);

                    if(status == true )
                        System.out.println("Batch Node Delete successful");
                    else
                        System.out.println("Batch Node Delete failed");
                    break;
                case 4:
                    //Task 2.13 Batch Edge Delete
                    System.out.print(" Enter NODEFILENAME GRAPHDBNAME :-");
                    inp = scanner.nextLine();
                    varargs = inp.split(" ");
                    if (dbExists(varargs[1]) ==1){
                        dbcreated=true;//check if DB file already exists
                    }
                    else dbcreated=false;

                    if(dbcreated!=true)//db doesnt exist
                    {
                        systemdef=new SystemDefs(varargs[1],1000,256,"Clock");
                        systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        //dbcreated=true;
                    }
                    else if (dbcreated==true && (systemdef==null)) {//db file exists,minibase restarted
                        SystemDefs.MINIBASE_RESTART_FLAG=true;
                        systemdef=new SystemDefs(varargs[1],1000,256,"Clock");
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                    }
                    status = batchedgedelete.edgedelete(varargs[0],varargs[1],systemdef);

                    if(status == true )
                        System.out.println("Batch Edge Delete successful");
                    else
                        System.out.println("Batch Edge Delete failed");
                    break;
                case 5:
                    //Task 2.14 Simple Node Query
                    System.out.print(" Enter GRAPHDBNAME NUMBUF QTYPE INDEX [QUERYOPTIONS] :-");
                    System.out.println(" If QTYPE = 0, [QUERYOPTIONS] = null :-");
                    System.out.println(" If QTYPE = 1, [QUERYOPTIONS] = null :-");
                    System.out.println(" If QTYPE = 2, [QUERYOPTIONS] = null :-");
                    System.out.println(" If QTYPE = 3, [QUERYOPTIONS] = TARGET_DESCRIPTOR DISTANCE :-");
                    System.out.println(" If QTYPE = 4, [QUERYOPTIONS] = LABEL :-");
                    System.out.println(" If QTYPE = 5, [QUERYOPTIONS] = TARGET_DESCRIPTOR DISTANCE :-");
                    inp = scanner.nextLine();
                    varargs = inp.split(" ");
                    if(dbcreated!=true)
                    {
                        systemdef=new SystemDefs(varargs[1],1000,256,"Clock");
                        systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                        dbcreated=true;
                    }
                    // if(Integer.parseInt(varargs[2])==0)
                    // {
                    //     Task14.executeQueryTypeZero(Integer.parseInt(varargs[3]),varargs[0],systemdef);
                    // }
                    // if(Integer.parseInt(varargs[2])==1)
                    // {
                    //     Task14.executeQueryTypeOne(Integer.parseInt(varargs[3]),varargs[0],systemdef);
                    // }
                    // if(Integer.parseInt(varargs[2])==4)
                    // {
                    //     Task14.executeQueryTypeFour(Integer.parseInt(varargs[3]),varargs[0],varargs[4],systemdef);
                    // }

//                        if(varargs.length == 4)
//                            res = nodequery.computequery(varargs[0],varargs[1],varargs[2],varargs[3]);
//                        else if(varargs.length == 5)
//                            res = nodequery.computequery(varargs[0],varargs[1],varargs[2],varargs[3],varargs[4]);
//                        else if(varargs.length == 6)
//                            res = nodequery.computequery(varargs[0],varargs[1],varargs[2],varargs[3],varargs[4],varargs[5]);
//                        System.out.println("Disk pages read ="+ res[0]);
                    //System.out.println("Disk pages written ="+ res[1]);
                    break;
                case 6:
                    //Task 2.14 Simple Edge Query
                    System.out.print(" Enter GRAPHDBNAME NUMBUF QTYPE INDEX [QUERYOPTIONS] :-");
                    System.out.println(" If QTYPE = 0, [QUERYOPTIONS] = null :-");
                    System.out.println(" If QTYPE = 1, [QUERYOPTIONS] = null :-");
                    System.out.println(" If QTYPE = 2, [QUERYOPTIONS] = null :-");
                    System.out.println(" If QTYPE = 3, [QUERYOPTIONS] = null :-");
                    System.out.println(" If QTYPE = 4, [QUERYOPTIONS] = null :-");
                    System.out.println(" If QTYPE = 5, [QUERYOPTIONS] = LOWER_WEIGHT_BOUND UPPER_WEIGHT_BOUND :-");
                    System.out.println(" If QTYPE = 6, [QUERYOPTIONS] = null :-");
                    inp = scanner.nextLine();
                    varargs = inp.split(" ");
                    if(dbcreated!=true)
                    {
                        systemdef=new SystemDefs(varargs[0],1000,256,"Clock");
                        systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                        dbcreated=true;
                    }
                    // if(Integer.parseInt(varargs[2])==0)
                    // {
                    //     task15_0.executeQueryTypeZero(Integer.parseInt(varargs[3]),varargs[0],systemdef);
                    // }
                    // if(Integer.parseInt(varargs[2])==1)
                    // {
                    //     task15_0.executeQueryTypeOne(Integer.parseInt(varargs[3]),varargs[0],systemdef);
                    // }
                    // if(Integer.parseInt(varargs[2])==2)
                    // {
                    //     task15_0.executeQueryTypeTwo(Integer.parseInt(varargs[3]),varargs[0],systemdef);
                    // }
                    // if(Integer.parseInt(varargs[2])==3)
                    // {
                    //     task15_0.executeQueryTypeThree(Integer.parseInt(varargs[3]),varargs[0],systemdef);
                    // }
                    // if(Integer.parseInt(varargs[2])==4)
                    // {
                    //     task15_0.executeQueryTypeFour(Integer.parseInt(varargs[3]),varargs[0],systemdef);
                    // }
                    // if(Integer.parseInt(varargs[2])==5)
                    // {
                    //     task15_0.executeQueryTypeFive(Integer.parseInt(varargs[3]),varargs[0],Integer.parseInt(varargs[4]),Integer.parseInt(varargs[5]),systemdef);
                    // }
                    // if(Integer.parseInt(varargs[2])==6)
                    // {
                    //     task15_0.executeQueryTypeSix(Integer.parseInt(varargs[3]),varargs[0],systemdef);
                    // }
                        /*if(varargs.length == 4)
                            res = edgequery.computequery(varargs[0],varargs[1],varargs[2],varargs[3]);
                        else if(varargs.length == 5)
                            res = edgequery.computequery(varargs[0],varargs[1],varargs[2],varargs[3],varargs[4]);
                        System.out.println("Disk pages read = "+ res[0]);
                        System.out.println("Disk pages written = "+ res[1]);*/
                    break;
                case 7:
                    System.out.print("Enter GRAPHDBNAME PathExpression:");
                    inp = scanner.nextLine();
                    varargs = inp.split(" ");

                    if (dbExists(varargs[0]) ==1){
                        dbcreated=true;//check if DB file already exists
                    }
                    else dbcreated=false;
                    if(dbcreated!=true)//db doesnt exist
                    {
                        systemdef=new SystemDefs(varargs[0],1000,256,"Clock");
                        systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        //dbcreated=true;
                    }
                    else if (dbcreated==true && (systemdef==null)) {//db file exists,minibase restarted
                        SystemDefs.MINIBASE_RESTART_FLAG=true;
                        systemdef=new SystemDefs(varargs[0],1000,256,"Clock");
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        systemdef.JavabaseDB.createIndexFiles(varargs[0]);
                    }
//                    String[] input= new String[3];
//                    input[0]="D:18,18,18,18,18";
//                    input[1]="L:930";
//                    input[2]="D:30,30,30,30,30";
//
                    String[] input = varargs[2].split("/");
                    PathExpressionQuery1 pq1 = new PathExpressionQuery1(input,systemdef.JavabaseDB);
                    pq1.fetchAllTailLabels(varargs[1]); //varargs[1] query type a,b,c
                    break;

                case 8:
                    System.out.print("Enter GRAPHDBNAME PathExpression:");
                    inp = scanner.nextLine();
                    varargs = inp.split(" ");
                    if (dbExists(varargs[0]) ==1){
                        dbcreated=true;//check if DB file already exists
                    }
                    else dbcreated=false;
                    if(dbcreated!=true)//db doesnt exist
                    {
                        systemdef=new SystemDefs(varargs[0],1000,256,"Clock");
                        systemdef.JavabaseDB.createIndexFiles(varargs[0]);
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        //dbcreated=true;
                    }
                    else if (dbcreated==true && (systemdef==null)) {//db file exists,minibase restarted
                        SystemDefs.MINIBASE_RESTART_FLAG=true;
                        systemdef=new SystemDefs(varargs[0],1000,256,"Clock");
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        systemdef.JavabaseDB.createIndexFiles(varargs[0]);
                    }
//                    //input2[0]="L:0";
//                    input2[0]="D:18,20,18,47,17";
////                  input[1]="L:41";
//                    //input2[1]="L:0_1000";
//                    input2[1]="W:40";
//                    //input[1]="L:930";

                    String[] input2= varargs[2].split("/");
                    PathExpressionQuery2 pq2 = new PathExpressionQuery2(input2,systemdef.JavabaseDB);
                    pq2.fetchAllTailLabels(varargs[1]);
                    break;

                case 9:
                    System.out.print("Enter GRAPHDBNAME PathExpression:");
                    inp = scanner.nextLine();
                    varargs = inp.split(" ");
                    if(dbcreated!=true)
                    {
                        systemdef=new SystemDefs(varargs[0],1000,256,"Clock");
                        systemdef.JavabaseDB.createIndexFiles(varargs[0]);
                        dbcreated=true;
                    }
                    String[] input3= new String[2];
                    input3[0]="L:0";
                    input3[1]="W:70";
                    
                    PathExpressionQuery3 pq3 = new PathExpressionQuery3(input3,systemdef.JavabaseDB);
                    pq3.fetchAllTailLabels();
                    break;

                case 10:
                    try {
                        systemdef.JavabaseBM.flushAllPages();
                    }
                    catch(PagePinnedException e){
                        continue;
                    }
                    systemdef.JavabaseDB.closeDB();
                    break;
            }

            //}
            //catch(Exception e)
            //{
            //    System.out.println("IOException due to invalid input");
            //    e.printStackTrace();
            //}
        }
    }
}

class GetStuff {
    GetStuff() {}

    public static int getChoice () {

        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));
        int choice = -1;

        try {
            choice = Integer.parseInt(in.readLine());
        }
        catch (NumberFormatException e) {
            e.printStackTrace();
            return -1;
        }
        catch (IOException e) {
            e.printStackTrace();
            return -1;
        }

        return choice;
    }

    public static void getReturn () {

        BufferedReader in = new BufferedReader (new InputStreamReader(System.in));

        try {
            String ret = in.readLine();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
public class GraphTest implements GlobalConst {
    public static void main(String[] args) {
        try{
            GraphDriver graphtest = new GraphDriver();
            graphtest.runTests();
        }
        catch (Exception e) {
            e.printStackTrace();
            System.err.println ("Error encountered during running Graph Query tests:\n");
            Runtime.getRuntime().exit(1);
        }
    }
}

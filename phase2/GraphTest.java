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
        System.out.println("[5] Path Expression Query 1");
        System.out.println("[6] Path Expression Query 2");
        System.out.println("[7] Path Expression Query 3");
        System.out.println("[8] Triangle Query");
        System.out.println("\n\n[9]  Quit!");
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

        while (choice != 9) {
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
                        systemdef=new SystemDefs(varargs[1],1000,1024,"Clock");
                        systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        //dbcreated=true;
                    }
                    else if (dbcreated==true && (systemdef==null)) {//db file exists,minibase restarted
                        SystemDefs.MINIBASE_RESTART_FLAG=true;
                        systemdef=new SystemDefs(varargs[1],1000,1024,"Clock");
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
                        systemdef=new SystemDefs(varargs[1],1000,1024,"Clock");
                        systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        //dbcreated=true;
                    }
                    else if (dbcreated==true && (systemdef==null)) {//db file exists,minibase restarted
                        SystemDefs.MINIBASE_RESTART_FLAG=true;
                        systemdef=new SystemDefs(varargs[1],1000,1024,"Clock");
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
                        systemdef=new SystemDefs(varargs[1],1000,1024,"Clock");
                        systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        //dbcreated=true;
                    }
                    else if (dbcreated==true && (systemdef==null)) {//db file exists,minibase restarted
                        SystemDefs.MINIBASE_RESTART_FLAG=true;
                        systemdef=new SystemDefs(varargs[1],1000,1024,"Clock");
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
                        systemdef=new SystemDefs(varargs[1],1000,1024,"Clock");
                        systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        //dbcreated=true;
                    }
                    else if (dbcreated==true && (systemdef==null)) {//db file exists,minibase restarted
                        SystemDefs.MINIBASE_RESTART_FLAG=true;
                        systemdef=new SystemDefs(varargs[1],1000,1024,"Clock");
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
                    System.out.print("Enter GRAPHDBNAME Querytype(a|b|c) PathExpression(Delim-/,L-labels,D-Descriptor):");
                    inp = scanner.nextLine();
                    varargs = inp.split(" ");

                    if (dbExists(varargs[0]) ==1){
                        dbcreated=true;//check if DB file already exists
                    }
                    else dbcreated=false;
                    if(dbcreated!=true)//db doesnt exist
                    {
                        systemdef=new SystemDefs(varargs[0],1000,1024,"Clock");
                        systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        //dbcreated=true;
                    }
                    else if (dbcreated==true && (systemdef==null)) {//db file exists,minibase restarted
                        SystemDefs.MINIBASE_RESTART_FLAG=true;
                        systemdef=new SystemDefs(varargs[0],1000,1024,"Clock");
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

                case 6:
                    System.out.print("Enter GRAPHDBNAME Querytype(a|b|c) PathExpression(Delim-/, L-node/edge labels,D-Descriptor,W-max_edge_weight):");
                    inp = scanner.nextLine();
                    varargs = inp.split(" ");
                    if (dbExists(varargs[0]) ==1){
                        dbcreated=true;//check if DB file already exists
                    }
                    else dbcreated=false;
                    if(dbcreated!=true)//db doesnt exist
                    {
                        systemdef=new SystemDefs(varargs[0],1000,1024,"Clock");
                        systemdef.JavabaseDB.createIndexFiles(varargs[0]);
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        //dbcreated=true;
                    }
                    else if (dbcreated==true && (systemdef==null)) {//db file exists,minibase restarted
                        SystemDefs.MINIBASE_RESTART_FLAG=true;
                        systemdef=new SystemDefs(varargs[0],1000,1024,"Clock");
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
                 case 7:
                     System.out.print("Enter GRAPHDBNAME Querytype(a|b|c) PathExpression(Delim-/, L-node label,D-Descriptor,W-max_edge_weight, D-Max_num_edges):");
                     inp = scanner.nextLine();
                     varargs = inp.split(" ");
                     if (dbExists(varargs[0]) ==1){
                         dbcreated=true;//check if DB file already exists
                     }
                     else dbcreated=false;
                     if(dbcreated!=true)//db doesnt exist
                     {
                         systemdef=new SystemDefs(varargs[0],1000,1024,"Clock");
                         systemdef.JavabaseDB.createIndexFiles(varargs[0]);
                         //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                         //System.out.println(systemdef.JavabaseDB.db_name());
                         //dbcreated=true;
                     }
                     else if (dbcreated==true && (systemdef==null)) {//db file exists,minibase restarted
                         SystemDefs.MINIBASE_RESTART_FLAG=true;
                         systemdef=new SystemDefs(varargs[0],1000,1024,"Clock");
                         //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                         //System.out.println(systemdef.JavabaseDB.db_name());
                         systemdef.JavabaseDB.createIndexFiles(varargs[0]);
                     }
                     String[] input3= varargs[2].split("/");
                     //String[] input3= new String[2];
                     //input3[0]="L:0";
                     //input3[1]="W:70";
                    
                     PathExpressionQuery3 pq3 = new PathExpressionQuery3(input3,systemdef.JavabaseDB);
                     pq3.fetchAllTailLabels(varargs[1]);
                     break;

                case 8:
                    System.out.print("Enter GRAPHDBNAME OPTION EN;EN;EN ");
                    inp = scanner.nextLine();
                    varargs = inp.split(" ");
                    if (dbExists(varargs[0]) ==1){
                        dbcreated=true;//check if DB file already exists
                    }
                    else dbcreated=false;
                    if(dbcreated!=true)//db doesnt exist
                    {
                        systemdef=new SystemDefs(varargs[0],1000,1024,"Clock");
                        systemdef.JavabaseDB.createIndexFiles(varargs[0]);
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        //dbcreated=true;
                    }
                    else if (dbcreated==true && (systemdef==null)) {//db file exists,minibase restarted
                        SystemDefs.MINIBASE_RESTART_FLAG=true;
                        systemdef=new SystemDefs(varargs[0],1000,1024,"Clock");
                        //System.out.println(systemdef.JavabaseDB.get_file_entry("NodeLabelsBtree_"+varargs[1]));
                        //System.out.println(systemdef.JavabaseDB.db_name());
                        systemdef.JavabaseDB.createIndexFiles(varargs[0]);
                    }
                    String input10 = varargs[1];
                    TriangleQuery tq = new TriangleQuery(systemdef,input10);

                    break;

                case 9:
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

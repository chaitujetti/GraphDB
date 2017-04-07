package phase2;

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

        System.out.println("\n\n[7]  Quit!");
        System.out.print("Hi, make your choice :");
    }

    public void runTests() throws Exception {
        int choice = -1;
        Scanner scanner = new Scanner(System.in);
        boolean status = false;
        menu();
        boolean dbcreated=false;
        SystemDefs systemdef=null;

        while (choice != 7) {
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
                        if(dbcreated!=true)
                        {
                            systemdef=new SystemDefs(varargs[1],1000,256,"Clock");
                            systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                            dbcreated=true;
                        }
                        status = batchnodeinsert.nodeinsert(varargs[0],varargs[1],systemdef);
                        if(status == true )
                            System.out.println("Batch Node Insert successful");
                        else
                            System.out.println("Batch Node Insert failed");
                        break;
                    case 2:
                        //Task 2.11 Batch Edge Insert
                        System.out.print(" Enter EDGEFILENAME GRAPHDBNAME :-");
                        inp = scanner.nextLine();
                        varargs = inp.split(" ");
                        if(dbcreated!=true)
                        {
                            systemdef=new SystemDefs(varargs[1],1000,256,"Clock");
                            systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                            dbcreated=true;
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
                        if(dbcreated!=true)
                        {
                            systemdef=new SystemDefs(varargs[1],1000,256,"Clock");
                            systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                            dbcreated=true;
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
                        status = batchedgedelete.edgedelete(varargs[0],varargs[1],systemdef);
                        if(dbcreated!=true)
                        {
                            systemdef=new SystemDefs(varargs[1],1000,256,"Clock");
                            systemdef.JavabaseDB.createIndexFiles(varargs[1]);
                            dbcreated=true;
                        }
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
                        if(Integer.parseInt(varargs[2])==0)
                        {
                            Task14.executeQueryTypeZero(Integer.parseInt(varargs[3]),varargs[0],systemdef);
                        }
                        if(Integer.parseInt(varargs[2])==1)
                        {
                            Task14.executeQueryTypeOne(Integer.parseInt(varargs[3]),varargs[0],systemdef);
                        }
                        if(Integer.parseInt(varargs[2])==4)
                        {
                            Task14.executeQueryTypeFour(Integer.parseInt(varargs[3]),varargs[0],varargs[4],systemdef);
                        }

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
                        if(Integer.parseInt(varargs[2])==0)
                        {
                            task15_0.executeQueryTypeZero(Integer.parseInt(varargs[3]),varargs[0],systemdef);
                        }
                        if(Integer.parseInt(varargs[2])==1)
                        {
                            task15_0.executeQueryTypeOne(Integer.parseInt(varargs[3]),varargs[0],systemdef);
                        }
                        if(Integer.parseInt(varargs[2])==2)
                        {
                            task15_0.executeQueryTypeTwo(Integer.parseInt(varargs[3]),varargs[0],systemdef);
                        }
                        if(Integer.parseInt(varargs[2])==3)
                        {
                            task15_0.executeQueryTypeThree(Integer.parseInt(varargs[3]),varargs[0],systemdef);
                        }
                        if(Integer.parseInt(varargs[2])==4)
                        {
                            task15_0.executeQueryTypeFour(Integer.parseInt(varargs[3]),varargs[0],systemdef);
                        }
                        if(Integer.parseInt(varargs[2])==5)
                        {
                            task15_0.executeQueryTypeFive(Integer.parseInt(varargs[3]),varargs[0],Integer.parseInt(varargs[4]),Integer.parseInt(varargs[5]),systemdef);
                        }
                        if(Integer.parseInt(varargs[2])==6)
                        {
                            task15_0.executeQueryTypeSix(Integer.parseInt(varargs[3]),varargs[0],systemdef);
                        }
                        /*if(varargs.length == 4)
                            res = edgequery.computequery(varargs[0],varargs[1],varargs[2],varargs[3]);
                        else if(varargs.length == 5)
                            res = edgequery.computequery(varargs[0],varargs[1],varargs[2],varargs[3],varargs[4]);
                        System.out.println("Disk pages read = "+ res[0]);
                        System.out.println("Disk pages written = "+ res[1]);*/
                        break;
                    case 7:
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

package phase2;

import diskmgr.*;
import global.*;
import heap.*;
import iterator.*;

/**
 * Created by vamsikrishnag on 4/24/17.
 */
public class TriangleTest implements GlobalConst {
    public TriangleTest(String filename, String[] query) throws Exception
    {

        String newquery = new String();
        newquery = query[1] +";"+ query[2] +";"+ query[3];
        Triangle t = new Triangle(filename,newquery);
        AttrType[] attr = new AttrType[3];
        for (int i=0;i<3;i++)
        {
            attr[i] = new AttrType(AttrType.attrString);
        }
        Tuple temp = new Tuple();
        temp = null;

        if(query[0].equals("A"))
        {
            System.out.println(" 9A Result is:");
            while ((temp = t.nlj.get_next()) != null)
            {
                try {
                    temp.print(attr);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

        else if(query[0].equals("B")) {
            System.out.println(" 9B Result is:");
            Sort sort = t.Sorting(t.nlj);
            while ((temp = sort.get_next()) != null)
            {
                try {
                    temp.print(attr);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
            sort.close();

        }
        else if(query[0].equals("C")) {
            System.out.println(" 9C Result is:");
            t.DuplicatesRemoval(t.nlj);
        }

        t.nlj.close();
        t.left.close();
    }

}

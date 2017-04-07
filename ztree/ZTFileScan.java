package ztree;

import global.*;
import java.io.*;



public class ZTFileScan extends IndexFileScan implements GlobalConst {
    ZTreeFile bfile;
    String treeFilename;
    ZTLeafPage leafPage;
    NID curNid;
    boolean didfirst;
    boolean deletedcurrent;

    KeyClass endkey;
    int keyType;
    int maxKeysize;

    @Override
    public DescriptorKeyDataEntry get_next() throws ScanIteratorException {

        DescriptorKeyDataEntry entry;
        PageId nextpage;
        try {
            if (leafPage == null)
                return null;

            if ((deletedcurrent && didfirst) || (!deletedcurrent && !didfirst)) {
                didfirst = true;
                deletedcurrent = false;
                entry=leafPage.getCurrent(curNid);
            }
            else {
                entry = leafPage.getNext(curNid);
            }

            while ( entry == null ) {
                nextpage = leafPage.getNextPage();
                SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), true);
                if (nextpage.pid == INVALID_PAGE) {
                    leafPage = null;
                    return null;
                }

                leafPage=new ZTLeafPage(nextpage, keyType);

                entry=leafPage.getFirst(curNid);
            }

            if (endkey != null)
                if ( ZT.keyCompare(entry.key, endkey)  > 0) {
                    // went past right end of scan
                    SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), false);
                    leafPage=null;
                    return null;
                }

            return entry;
        }
        catch ( Exception e) {
            e.printStackTrace();
            throw new ScanIteratorException();
        }
    }

    @Override
    public void delete_current() throws ScanDeleteException {
        DescriptorKeyDataEntry entry;
        try{
            if (leafPage == null) {
                System.out.println("No Record to delete!");
                throw new ScanDeleteException();
            }

            if( (deletedcurrent == true) || (didfirst==false) )
                return;

            entry=leafPage.getCurrent(curNid);
            SystemDefs.JavabaseBM.unpinPage( leafPage.getCurPage(), false);
            bfile.Delete(entry.key, ((LeafData)entry.data).getData());
            leafPage=bfile.findRunStart(entry.key, curNid);

            deletedcurrent = true;
            return;
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new ScanDeleteException();
        }

    }

    @Override
    public int keysize() {
        return maxKeysize;
    }

    public  void DestroyZTreeFileScan()
            throws  IOException, bufmgr.InvalidFrameNumberException,bufmgr.ReplacerException,
            bufmgr.PageUnpinnedException,bufmgr.HashEntryNotFoundException
    {
        if (leafPage != null) {
            SystemDefs.JavabaseBM.unpinPage(leafPage.getCurPage(), true);
        }
        leafPage=null;
    }
}
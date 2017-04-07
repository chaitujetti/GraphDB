package ztree;

import bufmgr.*;
import diskmgr.Page;
import global.*;

import java.io.*;


public class ZT implements GlobalConst {
   
    public final static int keyCompare(KeyClass key1, KeyClass key2) throws KeyNotMatchException {
        if (key1 instanceof DescriptorKey && key2 instanceof DescriptorKey) {
            return ((DescriptorKey) key1).getKey().compareTo(((DescriptorKey) key2).getKey());
        } else {
            throw new KeyNotMatchException(null, "Descriptor Key Types do not match");
        }
    }

    
    protected final static int getKeyLength(KeyClass key) throws KeyNotMatchException, IOException {
        if (key instanceof DescriptorKey) {
            return 10;
        } else {
            throw new KeyNotMatchException(null, "Descriptor key Types do not match");
        }
    }

    protected final static int getDataLength(short pageType) throws KeyNotMatchException {
        if ( pageType == NodeType.LEAF) {
            return 8;
        } else if ( pageType == NodeType.INDEX) {
            return 4;
        } else {
            throw new KeyNotMatchException(null, "Descriptor Key Types do not match");
        }
    }

    protected final static int getKeyDataLength(KeyClass key, short pageType)
            throws KeyNotMatchException, NodeNotMatchException, IOException {
        return getKeyLength(key) + getDataLength(pageType);
    }

    protected final static DescriptorKeyDataEntry getEntryFromBytes(byte[] from, int offset, int length,
                                                                 int keyType, short nodeType)
            throws KeyNotMatchException, NodeNotMatchException, ConvertException {
        KeyClass key;
        DataClass data;
        int n;
        try {
            if (nodeType == NodeType.INDEX) {
                n = 4;
                data = new IndexData(Convert.getIntValue(offset+length-4, from));
            }
            else if (nodeType == NodeType.LEAF) {
                n = 8;
                NID nid = new NID();
                nid.slotNo = Convert.getIntValue(offset+length-8, from);
                nid.pageNo = new PageId();
                nid.pageNo.pid = Convert.getIntValue(offset+length-4, from);
                data = new LeafData(nid);
            }
            else throw new NodeNotMatchException(null, "node types do not match");

            if (keyType == AttrType.attrDesc) {
                key = new DescriptorKey(Convert.getDescValue(offset, from));
            } else
                throw new KeyNotMatchException(null, "key types do not match");

            return new DescriptorKeyDataEntry(key, data);
        } catch ( IOException e) {
            throw new ConvertException(e, "conversion failed");
        }
    }

    protected final static byte[] getBytesFromEntry(DescriptorKeyDataEntry entry ) throws KeyNotMatchException,
            NodeNotMatchException, ConvertException {
        byte[] data;
        int n, m;
        try {
            n = getKeyLength(entry.key);
            m=n;
            if( entry.data instanceof IndexData )
                n+=4;
            else if (entry.data instanceof LeafData )
                n+=8;

            data=new byte[n];

            if ( entry.key instanceof DescriptorKey) {
                Convert.setDescValue(((DescriptorKey) entry.key).getDesc(),
                        0, data);
            } else {
                throw new KeyNotMatchException(null, "key types do not match");
            }

            if ( entry.data instanceof IndexData ) {
                Convert.setIntValue( ((IndexData)entry.data).getData().pid,
                        m, data);
            } else if ( entry.data instanceof LeafData ) {
                Convert.setIntValue( ((LeafData)entry.data).getData().slotNo,
                        m, data);
                Convert.setIntValue( ((LeafData)entry.data).getData().pageNo.pid,
                        m+4, data);

            } else {
                throw new NodeNotMatchException(null, "node types do not match");
            }
            return data;
        } catch (IOException e) {
            throw new  ConvertException(e, "convert failed");
        }
    }

    public static void printPage(PageId pageno, int keyType)
            throws IOException, IteratorException, ConstructPageException,
            HashEntryNotFoundException, ReplacerException,
            PageUnpinnedException, InvalidFrameNumberException {
        ZTSortedPage sortedPage = new ZTSortedPage(pageno, keyType);
        int i;
        i = 0;
        if (sortedPage.getType() == NodeType.INDEX) {
            ZTIndexPage indexPage = new ZTIndexPage((Page) sortedPage, keyType);
            System.out.println("");
            System.out.println("**************To Print an Index Page ********");
            System.out.println("Current Page ID: " + indexPage.getCurPage().pid);
            System.out.println("Left Link      : " + indexPage.getLeftLink().pid);

            NID nid = new NID();

            for (DescriptorKeyDataEntry entry = indexPage.getFirst(nid); entry != null;
                 entry = indexPage.getNext(nid)) {
                if (keyType == AttrType.attrDesc) {
                    System.out.println(i + " (key, pageId):   (" +
                            (DescriptorKey) entry.key + ",  " + (IndexData) entry.data + " )");
                }
                i++;
            }
            System.out.println("************** END ********");
            System.out.println("");
        } else if (sortedPage.getType() == NodeType.LEAF) {
            ZTLeafPage leafPage = new ZTLeafPage((Page) sortedPage, keyType);
            System.out.println("");
            System.out.println("**************To Print an Leaf Page ********");
            System.out.println("Current Page ID: " + leafPage.getCurPage().pid);
            System.out.println("Left Link      : " + leafPage.getPrevPage().pid);
            System.out.println("Right Link     : " + leafPage.getNextPage().pid);

            NID nid = new NID();

            for (DescriptorKeyDataEntry entry = leafPage.getFirst(nid); entry != null;
                 entry = leafPage.getNext(nid)) {
                if (keyType == AttrType.attrInteger)
                    System.out.println(i + " (key, [pageNo, slotNo]):   (" +
                            (DescriptorKey) entry.key + ",  " + (LeafData) entry.data + " )");
                i++;
            }
            System.out.println("************** END ********");
            System.out.println("");
        } else {
            System.out.println("Sorry!!! This page is neither Index nor Leaf page.");
        }
        SystemDefs.JavabaseBM.unpinPage(pageno, true/*dirty*/);
    }

    public static void printBTree(ZTreeHeaderPage header) throws IOException, ConstructPageException,
            IteratorException, HashEntryNotFoundException, InvalidFrameNumberException,
            PageUnpinnedException, ReplacerException {
        if(header.get_rootId().pid == INVALID_PAGE) {
            System.out.println("The Tree is Empty!!!");
            return;
        }

        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("---------------The B+ Tree Structure---------------");


        System.out.println(1 + "     " + header.get_rootId());

        _printTree(header.get_rootId(), "     ", 1, header.get_keyType());

        System.out.println("--------------- End ---------------");
        System.out.println("");
        System.out.println("");
    }

    private static void _printTree(PageId currentPageId, String prefix, int i,
                                   int keyType) throws IOException,
            ConstructPageException, IteratorException, HashEntryNotFoundException,
            InvalidFrameNumberException, PageUnpinnedException, ReplacerException {
        ZTSortedPage sortedPage = new ZTSortedPage(currentPageId, keyType);
        prefix = prefix+"       ";
        i++;
        if(sortedPage.getType() == NodeType.INDEX) {
            ZTIndexPage indexPage = new ZTIndexPage((Page)sortedPage, keyType);

            System.out.println(i+prefix+ indexPage.getPrevPage());
            _printTree( indexPage.getPrevPage(), prefix, i, keyType);

            NID nid=new NID();
            for(DescriptorKeyDataEntry entry=indexPage.getFirst(nid); entry!=null;
                 entry=indexPage.getNext(nid)) {
                System.out.println(i+prefix+(IndexData)entry.data);
                _printTree( ((IndexData)entry.data).getData(), prefix, i, keyType);
            }
        }
        SystemDefs.JavabaseBM.unpinPage(currentPageId , true/*dirty*/);
    }

    public static void printAllLeafPages(ZTreeHeaderPage header) throws IOException, ConstructPageException,
            IteratorException, HashEntryNotFoundException, InvalidFrameNumberException, PageUnpinnedException,
            ReplacerException {
        if (header.get_rootId().pid == INVALID_PAGE) {
            System.out.println("The Tree is Empty!!!");
            return;
        }

        System.out.println("");
        System.out.println("");
        System.out.println("");
        System.out.println("---------------The B+ Tree Leaf Pages---------------");


        _printAllLeafPages(header.get_rootId(), header.get_keyType());

        System.out.println("");
        System.out.println("");
        System.out.println("------------- All Leaf Pages Have Been Printed --------");
        System.out.println("");
        System.out.println("");
    }

    private static void _printAllLeafPages(PageId currentPageId,  int keyType) throws IOException,
            ConstructPageException, IteratorException, InvalidFrameNumberException, HashEntryNotFoundException,
            PageUnpinnedException, ReplacerException {
        ZTSortedPage sortedPage = new ZTSortedPage(currentPageId, keyType);

        if(sortedPage.getType() == NodeType.INDEX) {
            ZTIndexPage indexPage = new ZTIndexPage((Page)sortedPage, keyType);

            _printAllLeafPages( indexPage.getPrevPage(),  keyType);

            NID nid=new NID();
            for(DescriptorKeyDataEntry entry=indexPage.getFirst(nid); entry!=null;
                 entry=indexPage.getNext(nid)) {
                _printAllLeafPages( ((IndexData)entry.data).getData(),  keyType);
            }
        }

        if( sortedPage.getType()==NodeType.LEAF) {
            printPage(currentPageId, keyType);
        }
        SystemDefs.JavabaseBM.unpinPage(currentPageId , true/*dirty*/);
    }
}

package ztree;

import diskmgr.Page;
import global.*;
import heap.*;

import java.io.IOException;


public class ZTreeHeaderPage extends NHFPage {
    public void setPageId(PageId pageno) throws IOException {
        setCurPage(pageno);
    }

    public PageId getPageId() throws IOException {
        return getCurPage();
    }

    public void set_magic0( int magic ) throws IOException {
        setPrevPage(new PageId(magic));
    }

    public int get_magic0() throws IOException {
        return getPrevPage().pid;
    }

    void  set_rootId( PageId rootID ) throws IOException {
        setNextPage(rootID);
    }

    PageId get_rootId() throws IOException {
        return getNextPage();
    }

    void set_keyType( short key_type ) throws IOException {
        setSlot(3, (int)key_type, 0);
    }

    short get_keyType() throws IOException {
        return (short) getSlotLength(3);
    }

    void set_maxKeySize(int key_size ) throws IOException {
        setSlot(1, key_size, 0);
    }

    /** set the max keysize
     */
    int get_maxKeySize() throws IOException {
        return getSlotLength(1);
    }

    /** set the delete fashion
     */
    void set_deleteFashion(int fashion ) throws IOException {
        setSlot(2, fashion, 0);
    }

    /** get the delete fashion
     */
    int get_deleteFashion() throws IOException {
        return getSlotLength(2);
    }

    public ZTreeHeaderPage(PageId pageno) throws ConstructPageException {
        super();
        try {
            SystemDefs.JavabaseBM.pinPage(pageno, this, false/*Rdisk*/);
        }
        catch (Exception e) {
            throw new ConstructPageException(e, "pinpage failed");
        }
    }

    public ZTreeHeaderPage(Page page) {
        super(page);
    }

    public ZTreeHeaderPage( ) throws ConstructPageException {
        super();
        try{
            Page apage = new Page();
            PageId pageId = SystemDefs.JavabaseBM.newPage(apage,1);
            if (pageId == null) {
                throw new ConstructPageException(null, "new page failed");
            }
            this.init(pageId, apage);
        }
        catch (Exception e) {
            throw new ConstructPageException(e, "construct header page failed");
        }
    }
}

package ztree;

import global.*;

import java.io.IOException;


/**
 * Contains the enumerated types of state of the scan
 */
class ScanState 
{
  public static final int NEWSCAN = 0; 
  public static final int SCANRUNNING = 1; 
  public static final int SCANCOMPLETE = 2; 
}

/**
 * Base class for a index file
 */
public abstract class ZIndexFile
{

  abstract public void insert(final KeyClass data, final NID nid)
		  throws KeyTooLongException,
		  KeyNotMatchException,
		  LeafInsertRecException,
		  IndexInsertRecException,
		  ConstructPageException,
		  UnpinPageException,
		  PinPageException,
		  NodeNotMatchException,
		  ConvertException,
		  DeleteRecException,
		  IndexSearchException,
		  IteratorException,
		  LeafDeleteException,
		  InsertException,
		  IOException;

  
  abstract public boolean Delete(final KeyClass data, final NID rid)
		  throws DeleteFashionException,
		  LeafRedistributeException,
		  RedistributeException,
		  InsertRecException,
		  KeyNotMatchException,
		  UnpinPageException,
		  IndexInsertRecException,
		  FreePageException,
		  RecordNotFoundException,
		  PinPageException,
		  IndexFullDeleteException,
		  LeafDeleteException,
		  IteratorException,
		  ConstructPageException,
		  DeleteRecException,
		  IndexSearchException,
		  IOException;
}

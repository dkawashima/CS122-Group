package edu.caltech.nanodb.storage.heapfile;


import java.io.EOFException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.queryeval.ColumnStats;
import edu.caltech.nanodb.queryeval.ColumnStatsCollector;
import edu.caltech.nanodb.queryeval.TableStats;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;

import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.ColumnType;
import edu.caltech.nanodb.relations.SQLDataType;

import edu.caltech.nanodb.storage.DBFile;
import edu.caltech.nanodb.storage.DBPage;
import edu.caltech.nanodb.storage.FilePointer;
import edu.caltech.nanodb.storage.TupleFile;
import edu.caltech.nanodb.storage.InvalidFilePointerException;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.StorageManager;
import edu.caltech.nanodb.storage.TupleFileManager;


/**
 * This class implements the TupleFile interface for heap files. The storage
 * format utilizes a linked list of free blocks. The general structure is as
 * follows: Each page has a block pointer. A block pointer of -1 indicates that
 * there is no next node in the linked list. A block pointer of 0 indicates 
 * that block is not a part of the free block linked list. 
 *
 * When a tuple is to be inserted, the linked list is traversed and the tuple
 * is inserted into the first block with enough space to be found. If no block
 * is found, a new page is created. 
 * 
 * During the add tuple operation, we also check to see if the block is 'full.'
 * To determine if the block is full, we use the schema, and iterate though the 
 * columns to see how much space the tuple should take. If the block does not 
 * have enough space for another tuple, it is considered 'full.' If a block is
 * full and it is in the linked list, it is taken out of the linked list, 
 * and its next pointer is set to 0 to indicate that it is out of the list. 
 * 
 * During the delete operation, we check if the block now not full, using the
 * same schema method used for the add tuple operation. If a block is determined
 * to be not full, and it is not already in the linked list, we add it back to 
 * the linked list. 
 * 
 * During the update operation, we do both checks described above (the checks 
 * done for add and delete), this is because during an update, a tuple can 
 * either shrink or grow. Therefore, an update can both cause a full block
 * to become un-full, and an un-full block to become full. 
 */
public class HeapTupleFile implements TupleFile {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(HeapTupleFile.class);


    /**
     * The storage manager to use for reading and writing file pages, pinning
     * and unpinning pages, write-ahead logging, and so forth.
     */
    private StorageManager storageManager;


    /**
     * The manager for heap tuple files provides some higher-level operations
     * such as saving the metadata of a heap tuple file, so it's useful to
     * have a reference to it.
     */
    private HeapTupleFileManager heapFileManager;


    /** The schema of tuples in this tuple file. */
    private TableSchema schema;


    /** Statistics for this tuple file. */
    private TableStats stats;


    /** The file that stores the tuples. */
    private DBFile dbFile;


    public HeapTupleFile(StorageManager storageManager,
                         HeapTupleFileManager heapFileManager, DBFile dbFile,
                         TableSchema schema, TableStats stats) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        if (heapFileManager == null)
            throw new IllegalArgumentException("heapFileManager cannot be null");

        if (dbFile == null)
            throw new IllegalArgumentException("dbFile cannot be null");

        if (schema == null)
            throw new IllegalArgumentException("schema cannot be null");

        if (stats == null)
            throw new IllegalArgumentException("stats cannot be null");

        this.storageManager = storageManager;
        this.heapFileManager = heapFileManager;
        this.dbFile = dbFile;
        this.schema = schema;
        this.stats = stats;
    }


    @Override
    public TupleFileManager getManager() {
        return heapFileManager;
    }


    @Override
    public TableSchema getSchema() {
        return schema;
    }

    @Override
    public TableStats getStats() {
        return stats;
    }


    public DBFile getDBFile() {
        return dbFile;
    }


    /**
     * Returns the first tuple in this table file, or <tt>null</tt> if
     * there are no tuples in the file.
     */
    @Override
    public Tuple getFirstTuple() throws IOException {
        HeapFilePageTuple first = null;
        try {
            // Scan through the data pages until we hit the end of the table
            // file.  It may be that the first run of data pages is empty,
            // so just keep looking until we hit the end of the file.

            // Header page is page 0, so first data page is page 1.
page_scan:  // So we can break out of the outer loop from inside the inner one
            for (int iPage = 1; /* nothing */ ; iPage++) {
                // Look for data on this page.
                DBPage dbPage = storageManager.loadDBPage(dbFile, iPage);
                int numSlots = DataPage.getNumSlots(dbPage);
                for (int iSlot = 0; iSlot < numSlots; iSlot++) {
                    // Get the offset of the tuple in the page.  If it's 0 then
                    // the slot is empty, and we skip to the next slot.
                    int offset = DataPage.getSlotValue(dbPage, iSlot);
                    if (offset == DataPage.EMPTY_SLOT)
                        continue;
                    else
                        dbPage.unpin();
                    // This is the first tuple in the file.  Build up the
                    // HeapFilePageTuple object and return it.
                    first = new HeapFilePageTuple(schema, dbPage, iSlot, offset);
                    break page_scan;
                }
                dbPage.unpin();
            }
        }
        catch (EOFException e) {
            // We ran out of pages.  No tuples in the file!
            logger.debug("No tuples in table-file " + dbFile +
                         ".  Returning null.");
        }

        return first;
    }


    /**
     * Returns the tuple corresponding to the specified file pointer.  This
     * method is used by many other operations in the database, such as
     * indexes.
     *
     * @throws InvalidFilePointerException if the specified file-pointer
     *         doesn't actually point to a real tuple.
     */
    @Override
    public Tuple getTuple(FilePointer fptr)
        throws InvalidFilePointerException, IOException {

        DBPage dbPage;
        try {
            // This could throw EOFException if the page doesn't actually exist.
            dbPage = storageManager.loadDBPage(dbFile, fptr.getPageNo());
        }
        catch (EOFException eofe) {
            throw new InvalidFilePointerException("Specified page " +
                fptr.getPageNo() + " doesn't exist in file " +
                dbFile.getDataFile().getName(), eofe);
        }

        // The file-pointer points to the slot for the tuple, not the tuple itself.
        // So, we need to look up that slot's value to get to the tuple data.

        int slot;
        try {
            slot = DataPage.getSlotIndexFromOffset(dbPage, fptr.getOffset());
        }
        catch (IllegalArgumentException iae) {
            throw new InvalidFilePointerException(iae);
        }

        // Pull the tuple's offset from the specified slot, and make sure
        // there is actually a tuple there!

        int offset = DataPage.getSlotValue(dbPage, slot);
        if (offset == DataPage.EMPTY_SLOT) {
            throw new InvalidFilePointerException("Slot " + slot +
                " on page " + fptr.getPageNo() + " is empty.");
        }

        if (dbPage.isPinned()) {
            dbPage.unpin();
        }
        return new HeapFilePageTuple(schema, dbPage, slot, offset);
    }


    /**
     * Returns the tuple that follows the specified tuple, or {@code null} if
     * there are no more tuples in the file.  This method must operate
     * correctly regardless of whether the input tuple is pinned or
     * ned.
     *
     * @param tup the "previous tuple" that specifies where to start looking
     *        for the next tuple
     */
    @Override
    public Tuple getNextTuple(Tuple tup) throws IOException {

        /* Procedure:
         *   1)  Get slot index of current tuple.
         *   2)  If there are more slots in the current page, find the next
         *       non-empty slot.
         *   3)  If we get to the end of this page, go to the next page
         *       and try again.
         *   4)  If we get to the end of the file, we return null.
         */

        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        // Retrieve the location info from the previous tuple.  Since the
        // tuple (and/or its backing page) may already have a pin-count of 0,
        // we can't necessarily use the page itself.
        DBPage prevDBPage = ptup.getDBPage();
        DBFile dbFile = prevDBPage.getDBFile();
        int prevPageNo = prevDBPage.getPageNo();
        int prevSlot = ptup.getSlot();

        // Retrieve the page itself so that we can access the internal data.
        // The page will come back pinned on behalf of the caller.  (If the
        // page is still in the Buffer Manager's cache, it will not be read
        // from disk, so this won't be expensive in that case.)
        DBPage dbPage = storageManager.loadDBPage(dbFile, prevPageNo);
        HeapFilePageTuple nextTup = null;

        // Start by looking at the slot immediately following the previous
        // tuple's slot.
        int nextSlot = prevSlot + 1;

page_scan:  // So we can break out of the outer loop from inside the inner loop.
        while (true) {
            int numSlots = DataPage.getNumSlots(dbPage);

            while (nextSlot < numSlots) {
                int nextOffset = DataPage.getSlotValue(dbPage, nextSlot);
                if (nextOffset != DataPage.EMPTY_SLOT) {
                    // Creating this tuple will pin the page a second time.
                    nextTup = new HeapFilePageTuple(schema, dbPage, nextSlot,
                                                    nextOffset);
                    break page_scan;
                }

                nextSlot++;
            }

            dbPage.unpin();
            // If we got here then we reached the end of this page with no
            // tuples.  Go on to the next data-page, and start with the first
            // tuple in that page.

            try {
                dbPage = storageManager.loadDBPage(dbFile, dbPage.getPageNo() + 1);
                nextSlot = 0;
            }
            catch (EOFException e) {
                // Hit the end of the file with no more tuples.  We are done
                // scanning.
                break;
            }
        }
        if (dbPage.isPinned()) {
            dbPage.unpin();
        }
        return nextTup;
    }


    /**
     * Adds the specified tuple into the table file.  A new
     * <tt>HeapFilePageTuple</tt> object corresponding to the tuple is returned.
     *
     * @review (donnie) This could be made a little more space-efficient.
     *         Right now when computing the required space, we assume that we
     *         will <em>always</em> need a new slot entry, whereas the page may
     *         contain empty slots.  (Note that we don't always create a new
     *         slot when adding a tuple; we will reuse an empty slot.  This
     *         inefficiency is simply in estimating the size required for the
     *         new tuple.)
     */
    @Override
    public Tuple addTuple(Tuple tup) throws IOException {

        /*
         * Check to see whether any constraints are violated by
         * adding this tuple
         *
         * Find out how large the new tuple will be, so we can find a page to
         * store it.
         *
         * Find a page with space for the new tuple.
         *
         * Generate the data necessary for storing the tuple into the file.
         */

        int tupSize = PageTuple.getTupleStorageSize(schema, tup);
        logger.debug("Adding new tuple of size " + tupSize + " bytes.");

        // Sanity check:  Make sure that the tuple would actually fit in a page
        // in the first place!
        // The "+ 2" is for the case where we need a new slot entry as well.
        if (tupSize + 2 > dbFile.getPageSize()) {
            throw new IOException("Tuple size " + tupSize +
                " is larger than page size " + dbFile.getPageSize() + ".");
        }

        // Search for a page to put the tuple in.  If we hit the end of the
        // data file, create a new page.


        // Load the header page and access the free block linked list pointers.
        DBPage headerpage = storageManager.loadDBPage(dbFile, 0);
        int begin_list_pointer =
            headerpage.readInt(HeaderPage.OFFSET_BEGIN_PTR_START);
        int prevPageNo = 0;
        int pageNo = 0;
        DBPage dbPage = null;

        // o might not need..... if else.
        if(begin_list_pointer == -1) {
            // make new page

            pageNo = dbFile.getNumPages();
            logger.debug("Creating new page " + pageNo + " to store new tuple.");
            dbPage = storageManager.loadDBPage(dbFile, pageNo, true);
            DataPage.initNewPage(dbPage);


            dbPage.writeInt(DataPage.getTupleDataEnd(dbPage), -1);
            headerpage.writeInt(HeaderPage.OFFSET_BEGIN_PTR_START, pageNo);

        }
        else {
            pageNo = begin_list_pointer;


            while (true) {
                // Try to load the page without creating a new one.
                if(pageNo != -1) {
                    try { // Just in case linked list does not point
                          // to valid block


                        dbPage = storageManager.loadDBPage(dbFile, pageNo);


                    }
                    catch (EOFException eofe) {
                        // Couldn't load the current page, because it doesn't exist.
                        // Break out of the loop.
                        logger.debug("Reached invalid block.");
                        break;
                    }
                }
                else {
                    // Couldn't load the current page, because it doesn't exist.
                    // Break out of the loop.
                    logger.debug("Reached end of free block list without " +
                                 "finding space for new tuple.");


                    break;
                }


                int freeSpace = DataPage.getFreeSpaceInPage(dbPage);

                logger.trace(String.format("Page %d has %d bytes of free space.",
                             pageNo, freeSpace));



                // If this page has enough free space to add a new tuple, break
                // out of the loop.  (The "+ 2" is for the new slot entry we will
                // also need.)
                if (freeSpace >= tupSize + 2) {
                    logger.debug("Found space for new tuple in page " + pageNo + ".");




                    break;
                }

                // If we reached this point then the page doesn't have enough
                // space, so go on to the next data page.


                prevPageNo = pageNo;

                pageNo = dbPage.readInt(DataPage.getTupleDataEnd(dbPage));



                dbPage.unpin();
                dbPage = null;  // So the next section will work properly.

            }

        }

        if (dbPage == null) {
            // Try to create a new page, and add it to the start of the free
            // block linked list
            pageNo = dbFile.getNumPages();
            logger.debug("Creating new page " + pageNo + " to store new tuple.");
            dbPage = storageManager.loadDBPage(dbFile, pageNo, true);
            DataPage.initNewPage(dbPage);

            int old_head = headerpage.readInt(HeaderPage.OFFSET_BEGIN_PTR_START);
            headerpage.writeInt(HeaderPage.OFFSET_BEGIN_PTR_START, pageNo);

            dbPage.writeInt(DataPage.getTupleDataEnd(dbPage), old_head);

        }
        headerpage.unpin();

        int slot = DataPage.allocNewTuple(dbPage, tupSize);
        int tupOffset = DataPage.getSlotValue(dbPage, slot);

        logger.debug(String.format(
            "New tuple will reside on page %d, slot %d.", pageNo, slot));

        HeapFilePageTuple pageTup =
            HeapFilePageTuple.storeNewTuple(schema, dbPage, slot, tupOffset, tup);

        // If the dbPage is 'full,' remove it from the free block list

        // Get the max space a tuple can take, based on the schema
        List<ColumnInfo> col_infos = getSchema().getColumnInfos();
        int calculated_tuple_size = 0;
        for(ColumnInfo col_info : col_infos) {

            ColumnType col_type = col_info.getType();
            SQLDataType sqltype =  col_type.getBaseType();
            calculated_tuple_size += PageTuple.getStorageSize(col_type, 0);
        }

        if(DataPage.getFreeSpaceInPage(dbPage) < calculated_tuple_size) {
            DBPage prevdbPage = storageManager.loadDBPage(dbFile,
                prevPageNo);
            int nextdbPageNo = dbPage.readInt(DataPage.getTupleDataEnd(dbPage));

            if(prevPageNo != 0) {
                prevdbPage.writeInt(DataPage.getTupleDataEnd(dbPage), nextdbPageNo);
            }
            else {
                prevdbPage.writeInt(HeaderPage.OFFSET_BEGIN_PTR_START, nextdbPageNo);
            }

            dbPage.writeInt(DataPage.getTupleDataEnd(dbPage), 0);
            prevdbPage.unpin();
            // Log change to write-ahead log
            storageManager.logDBPageWrite(prevdbPage);
        }

        DataPage.sanityCheck(dbPage);
        dbPage.unpin();
        // Log change to write-ahead log
        storageManager.logDBPageWrite(dbPage);
        // Log change to write-ahead log
        storageManager.logDBPageWrite(headerpage);
        return pageTup;
    }


    // Inherit interface-method documentation.
    /**
     * @review (donnie) This method will fail if a tuple is modified in a way
     *         that requires more space than is currently available in the data
     *         page.  One solution would be to move the tuple to a different
     *         page and then perform the update, but that would cause all kinds
     *         of additional issues.  So, if the page runs out of data, oh well.
     */
    @Override
    public void updateTuple(Tuple tup, Map<String, Object> newValues)
        throws IOException {

        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        for (Map.Entry<String, Object> entry : newValues.entrySet()) {
            String colName = entry.getKey();
            Object value = entry.getValue();

            int colIndex = schema.getColumnIndex(colName);
            ptup.setColumnValue(colIndex, value);
        }

        DBPage dbPage = ptup.getDBPage();

        // Get the max space a tuple can take, based on the schema
        List<ColumnInfo> col_infos = getSchema().getColumnInfos();
        int calculated_tuple_size = 0;
        for(ColumnInfo col_info : col_infos) {

            ColumnType col_type = col_info.getType();
            SQLDataType sqltype =  col_type.getBaseType();
            calculated_tuple_size += PageTuple.getStorageSize(col_type, 0);
        }

        int nextdbPageNo = dbPage.readInt(DataPage.getTupleDataEnd(dbPage));

        if(DataPage.getFreeSpaceInPage(dbPage) < calculated_tuple_size && 
            nextdbPageNo != 0) {

            // Remove dbpage from list
            DBPage headerpage = storageManager.loadDBPage(dbFile, 0);
            int begin_list_pointer =
                headerpage.readInt(HeaderPage.OFFSET_BEGIN_PTR_START);
            int prevPageNo = 0;
            int pageNo = begin_list_pointer;
            DBPage curPage = null;

            while(pageNo != dbPage.getPageNo()) {
                prevPageNo = pageNo;

                curPage = storageManager.loadDBPage(dbFile, pageNo);

                pageNo = curPage.readInt(DataPage.getTupleDataEnd(dbPage));

            }

            //nextdbPageNo = dbPage.readInt(DataPage.getTupleDataEnd(dbPage));
            // A next = 0 indicates that the block is out of the list
            dbPage.writeInt(DataPage.getTupleDataEnd(dbPage), 0);

            // curPage is the header page
            if(curPage == null) {
                headerpage.writeInt(HeaderPage.OFFSET_BEGIN_PTR_START, nextdbPageNo);
                // Log change to write-ahead log
                storageManager.logDBPageWrite(headerpage);
            }
            else {
                curPage.writeInt(DataPage.getTupleDataEnd(curPage), nextdbPageNo);
                // Log change to write-ahead log
                storageManager.logDBPageWrite(curPage);
            }
        }
        else if(DataPage.getFreeSpaceInPage(dbPage) >= calculated_tuple_size &&
            nextdbPageNo == 0) {
            DBPage headerpage = storageManager.loadDBPage(dbFile, 0);
            int begin_list_pointer =
                headerpage.readInt(HeaderPage.OFFSET_BEGIN_PTR_START);
            dbPage.writeInt(DataPage.getTupleDataEnd(dbPage), begin_list_pointer);
            headerpage.writeInt(HeaderPage.OFFSET_BEGIN_PTR_START, dbPage.getPageNo());
            headerpage.unpin();
            // Log change to write-ahead log
            storageManager.logDBPageWrite(headerpage);
        }
        // Log change to write-ahead log
        storageManager.logDBPageWrite(dbPage);

        DataPage.sanityCheck(dbPage);
    }


    // Inherit interface-method documentation.
    @Override
    public void deleteTuple(Tuple tup) throws IOException {

        if (!(tup instanceof HeapFilePageTuple)) {
            throw new IllegalArgumentException(
                "Tuple must be of type HeapFilePageTuple; got " + tup.getClass());
        }
        HeapFilePageTuple ptup = (HeapFilePageTuple) tup;

        DBPage dbPage = ptup.getDBPage();

        // Get the max space a tuple can take, based on the schema
        List<ColumnInfo> col_infos = getSchema().getColumnInfos();
        int calculated_tuple_size = 0;
        for(ColumnInfo col_info : col_infos) {

            ColumnType col_type = col_info.getType();
            SQLDataType sqltype =  col_type.getBaseType();
            calculated_tuple_size += PageTuple.getStorageSize(col_type, 0);
        }

        int nextdbPageNo = dbPage.readInt(DataPage.getTupleDataEnd(dbPage));
        if (DataPage.getFreeSpaceInPage(dbPage) >= calculated_tuple_size &&
                nextdbPageNo == 0){
            // Add newly free block to free block list.
            DBPage headerpage = storageManager.loadDBPage(dbFile, 0);
            int begin_list_pointer =
                    headerpage.readInt(HeaderPage.OFFSET_BEGIN_PTR_START);
            dbPage.writeInt(DataPage.getTupleDataEnd(dbPage), begin_list_pointer);
            headerpage.writeInt(HeaderPage.OFFSET_BEGIN_PTR_START, dbPage.getPageNo());
            headerpage.unpin();
            // Log change to write-ahead log
            storageManager.logDBPageWrite(headerpage);
        }
        DataPage.deleteTuple(dbPage, ptup.getSlot());
        DataPage.sanityCheck(dbPage);
        ptup.unpin();
        // Log change to write-ahead log
        storageManager.logDBPageWrite(dbPage);
        // Note that we don't invalidate the page-tuple when it is deleted,
        // so that the tuple can still be unpinned, etc.
    }


    @Override
    public void analyze() throws IOException {
        // Scan through the data pages until we hit the end of the table
        // file.  It may be that the first run of data pages is empty,
        // so just keep looking until we hit the end of the file.

        // Keep track of the current tuple we are analyzing
        HeapFilePageTuple current_tuple;

        // Keeps track of the total table size, number of tuples, and number of pages
        float totalSize = 0;
        int numTuples = 0;
        int numDataPages = 0;

        // Keep an array of each column's statistics
        ColumnStatsCollector[] columnStatsCollectors = new ColumnStatsCollector [getSchema().numColumns()];

        for (int iColumn = 0; iColumn < getSchema().numColumns(); iColumn++) {

            SQLDataType theType = getSchema().getColumnInfo(iColumn).getType().getBaseType();
            columnStatsCollectors[iColumn] = new ColumnStatsCollector(theType);
        }
        // Header page is page 0, so the first data page is page 1.
        for (int iPage = 1; /* loop until no pages left */ ; iPage++) {

            // Look for the next tuple on the next page, if it exists.
            try {
                DBPage dbPage = storageManager.loadDBPage(dbFile, iPage);
                numDataPages++;
                totalSize += DataPage.getTupleDataEnd(dbPage) - DataPage.getTupleDataStart(dbPage);
                int numSlots = DataPage.getNumSlots(dbPage);
                for (int iSlot = 0; iSlot < numSlots; iSlot++) {

                    // Get the offset of the tuple in the page.  If it's 0 then
                    // the slot is empty, and we skip to the next slot.
                    int offset = DataPage.getSlotValue(dbPage, iSlot);
                    if (offset == DataPage.EMPTY_SLOT)
                        continue;

                    numTuples++;
                    // This is the next tuple in the file.  Build up the HeapFilePageTuple object and analyze it.
                    current_tuple = new HeapFilePageTuple(schema, dbPage, iSlot, offset);

                    // Loop through all of the columns, adding the value to the corresponding ColumnStatsCollector
                    for (int iColumn = 0; iColumn < getSchema().numColumns(); iColumn++) {
                        columnStatsCollectors[iColumn].addValue(current_tuple.getColumnValue(iColumn));
                    }
                }
                dbPage.unpin();
            }
            catch (EOFException e) {
                // Hit the end of the file with no more tuples.  We are done
                // scanning.
                break;
            }
        }

        // Collect all of the information necessary to pass into a new TableStats object
        // Calculate the average tuple size
        float avgTupleSize = totalSize / ((float)numTuples);

        // Fill in an ArrayList object with the correct ColumnStats, made from the array of ColumnStatsCollector's
        ArrayList<ColumnStats> columnStats = new ArrayList<ColumnStats>();
        for (int iColumnStat = 0; iColumnStat < columnStatsCollectors.length; iColumnStat++) {
            columnStats.add(columnStatsCollectors[iColumnStat].getColumnStats());
        }
        TableStats tablestats = new TableStats(numDataPages, numTuples, avgTupleSize, columnStats);

        // Store the TableStats object into this object's "stats" field
        this.stats = tablestats;

        // Call the method saveMetaData from the HeapTupleFileManager class
        heapFileManager.saveMetadata(this);

    }


    @Override
    public List<String> verify() throws IOException {
        // TODO!
        // Right now we will just report that everything is fine.
        return new ArrayList<String>();
    }


    @Override
    public void optimize() throws IOException {
        throw new UnsupportedOperationException("Not yet implemented!");
    }
}

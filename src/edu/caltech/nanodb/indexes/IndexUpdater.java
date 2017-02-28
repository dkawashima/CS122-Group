package edu.caltech.nanodb.indexes;


import java.io.IOException;

import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.relations.ColumnRefs;
import edu.caltech.nanodb.relations.TableConstraintType;
import edu.caltech.nanodb.relations.TableInfo;
import edu.caltech.nanodb.relations.TableSchema;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.server.EventDispatchException;
import edu.caltech.nanodb.server.RowEventListener;
import edu.caltech.nanodb.storage.TupleFile;
import edu.caltech.nanodb.storage.PageTuple;
import edu.caltech.nanodb.storage.StorageManager;


/**
 * This class implements the {@link RowEventListener} interface to make sure
 * that all indexes on an updated table are kept up-to-date.  This handler is
 * installed by the {@link StorageManager#initialize} setup method.
 */
public class IndexUpdater implements RowEventListener {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(IndexUpdater.class);


    /**
     * A cached reference to the index manager since we use it a lot in this
     * class.
     */
    private IndexManager indexManager;


    public IndexUpdater(StorageManager storageManager) {
        if (storageManager == null)
            throw new IllegalArgumentException("storageManager cannot be null");

        this.indexManager = storageManager.getIndexManager();
    }


    @Override
    public void beforeRowInserted(TableInfo tblFileInfo, Tuple newValues) {
        // Ignore.
    }


    @Override
    public void afterRowInserted(TableInfo tblFileInfo, Tuple newTuple) {

        if (!(newTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                "newTuple must be castable to PageTuple");
        }

        // Add the new row to any indexes on the table.
        addRowToIndexes(tblFileInfo, (PageTuple) newTuple);
    }

    @Override
    public void beforeRowUpdated(TableInfo tblFileInfo, Tuple oldTuple,
                                 Tuple newValues) {

        if (!(oldTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                "oldTuple must be castable to PageTuple");
        }

        // Remove the old row from any indexes on the table.
        removeRowFromIndexes(tblFileInfo, (PageTuple) oldTuple);
    }

    @Override
    public void afterRowUpdated(TableInfo tblFileInfo, Tuple oldValues,
                                Tuple newTuple) {

        if (!(newTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                "newTuple must be castable to PageTuple");
        }

        // Add the new row to any indexes on the table.
        addRowToIndexes(tblFileInfo, (PageTuple) newTuple);
    }

    @Override
    public void beforeRowDeleted(TableInfo tblFileInfo, Tuple oldTuple) {
        if (!(oldTuple instanceof PageTuple)) {
            throw new IllegalArgumentException(
                "oldTuple must be castable to PageTuple");
        }

        // Remove the old row from any indexes on the table.
        removeRowFromIndexes(tblFileInfo, (PageTuple) oldTuple);
    }

    @Override
    public void afterRowDeleted(TableInfo tblFileInfo, Tuple oldValues) {
        // Ignore.
    }


    /**
     * This helper method handles the case when a tuple is being added to the
     * table, after the row has already been added to the table.  All indexes
     * on the table are updated to include the new row.
     *
     * @param tblFileInfo details of the table being updated
     *
     * @param ptup the new tuple that was inserted into the table
     */
    private void addRowToIndexes(TableInfo tblFileInfo, PageTuple ptup) {
        logger.debug("Adding tuple " + ptup + " to indexes for table " +
            tblFileInfo.getTableName());

        System.out.println("\n\nHELLO THERE!!!!\nfunction: addRowToIndexes()\n"); // Debug


        // Iterate over the index tables for the table.
        // ColumnRefs is a set of columns
        TableSchema schema = tblFileInfo.getSchema();
        for (ColumnRefs indexDef : schema.getIndexes().values()) {
            try {
                IndexInfo indexInfo = indexManager.openIndex(tblFileInfo,
                    indexDef.getIndexName());

                System.out.println("The ColumnRefs:");
                System.out.println(indexDef.toString());
                System.out.println();

                // Create a search key based on the ColumnRefs indexDef - doesn't include tuple's
                // file pointer so cannot be used for insertion
                TupleLiteral searchKey = IndexUtils.makeTableSearchKey(indexDef, (Tuple) ptup, false);


                // Check Unique index: column the index is on cannot have duplicate
                // values. Multiple NULLs are allowed. 

                /* TODO: ask TA
                The contents of this if-statement seem to be useless. 

                CREATE TABLE t (
                          a INTEGER,
                          b VARCHAR(30) UNIQUE,
                          c FLOAT
                );
                insert into t values(1, 'bob lee', 3.2);
                insert into t values(3, 'bob lee', 3.2);

                The exception is caught without it.
                */
                if(indexDef.getConstraintType() == TableConstraintType.UNIQUE) {
                    System.out.println("Unique constraint on column set");

                    // If there is a match in the index of the search key, cannot add row.
                    if(IndexUtils.findTupleInIndex((Tuple) searchKey, indexInfo.getTupleFile()) != null) {
                        
                        System.out.println("\nThe tuple that already exists in the index");
                        System.out.println(IndexUtils.findTupleInIndex((Tuple) searchKey, indexInfo.getTupleFile()).toString());

                        // TODO: ask TA what correct way to handle error is
                        logger.error("Cannot add tuple:" + ptup + " Uniqueness constraint would be violated.");
                        
                        System.out.println("\nUnique column!!!\n");

                        return;
                    }
                }

                // Add a new tuple to the index, including the
                // tuple-pointer to the tuple in the table.
                TupleLiteral newIndexTuple = IndexUtils.makeTableSearchKey(indexDef, (Tuple) ptup, true);
                indexInfo.getTupleFile().addTuple(newIndexTuple);

            }
            catch (IOException e) {
                throw new EventDispatchException("Couldn't update index " +
                    indexDef.getIndexName() + " for table " +
                    tblFileInfo.getTableName(), e);
            }
        }
    }


    /**
     * This helper method handles the case when a tuple is being removed from
     * the table, before the row has actually been removed from the table.
     * All indexes on the table are updated to remove the row.
     *
     * @param tblFileInfo details of the table being updated
     *
     * @param ptup the tuple about to be removed from the table
     */
    private void removeRowFromIndexes(TableInfo tblFileInfo, PageTuple ptup) {

        logger.debug("Removing tuple " + ptup + " from indexes for table " +
            tblFileInfo.getTableName());


        System.out.println("\n\nGREETINGS!!!\nfunction: removeRowFromIndexes()\n"); // Debug


        // Iterate over the indexes in the table.
        TableSchema schema = tblFileInfo.getSchema();
        for (ColumnRefs indexDef : schema.getIndexes().values()) {
            try {
                IndexInfo indexInfo = indexManager.openIndex(tblFileInfo,
                    indexDef.getIndexName());

                // Find ptup's corresponding row in the index. 
                //      Search key contains the file-pointer;
                //      we don't just want the index row that matches cols, since its possible that the 
                //      columns do not have uniqueness constraint.
                TupleLiteral searchKey = IndexUtils.makeTableSearchKey(indexDef, (Tuple) ptup, true);
                PageTuple indexTupleToDelete = IndexUtils.findTupleInIndex((Tuple) searchKey, indexInfo.getTupleFile());

                // If indexTupleToDelete does not exist, the index is bad.
                if(indexTupleToDelete == null) {
                    throw new IllegalStateException("Index tuple corresponding to the to-be-deleted row is missing.");
                }
                // If the index tuple is found, delete it.
                else {
                    indexInfo.getTupleFile().deleteTuple((Tuple) indexTupleToDelete);
                }
                

            }
            catch (IOException e) {
                throw new EventDispatchException("Couldn't update index " +
                    indexDef.getIndexName() + " for table " +
                    tblFileInfo.getTableName());
            }
        }
    }
}

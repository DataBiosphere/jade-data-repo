package bio.terra.common;

import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.RowIdMatch;
import bio.terra.service.dataset.Dataset;

import java.util.List;

/**
 * In the long term, we want to make the primary data store be pluggable, supporting different implementations.
 * This interface documents what that pluggable interface might look like. Of course, we never know until we
 * build another one.
 *
 * We use the term "container" for the object that contains a set of tables. In BigQuery, this would be a
 * BigQuery snapshot. In Postgresql, it would probably be a schema.
 *
 * The expected sequence of calls to ensure idempotence:
 * <ul>
 *     <li>do sequence</li>
 *     <ol>
 *         <li>datasetExists - validate that the dataset does not exist before calling create</li>
 *         <li>createDataset - if it finds the dataset, it deletes it and recreates. That covers the case where the
 *         create operation is interrupted by a service failure and restarts.</li>
 *     </ol>
 *     <li>undo</li>
 *     <ol>
 *         <li>deleteDataset - called on undo; does not fail if the dataset has already been deleted</li>
 *     </ol>
 * </ul>
 *
 * All implementation-specific exceptions should be caught by the implementation and wrapped
 * in PdaoException.
 */
public interface PrimaryDataAccess {

    /**
     * Create the container and tables for a dataset.
     * BigQuery: container is a BigQuery snapshot
     *
     * @param dataset
     */
    void createDataset(Dataset dataset);

    /**
     * Delete the dataset. All tables within the container and the container are deleted
     *
     * @param dataset
     * @return true if the dataset was deleted; false if it was not found; throw on other errors
     */
    boolean deleteDataset(Dataset dataset);

    /**
     * Given inputs from one asset, compute the row ids from the input values. The
     * returned structure provides a list suitable to pass into createSnapshot and
     * information to return meaningful errors for mismatched input values.
     *
     * @param snapshot
     * @param source - source in the snapshot we are mapping
     * @param inputValues
     * @return RowIdMatch
     */
    RowIdMatch mapValuesToRows(Snapshot snapshot,
                               SnapshotSource source,
                               List<String> inputValues);

    /**
     * Create the container, tables and views for a snapshot.
     * BigQuery: container is a BigQuery snapshot
     * @param snapshot
     * @param rowIds - row ids for the root table
     */
    void createSnapshot(Snapshot snapshot, List<String> rowIds);

    /**
     * Delete the snapshot. All tables within the container and the container are deleted
     *
     * @param snapshot
     * @return true if the snapshot was deleted; false if it was not found; throw on other errors
     */
    boolean deleteSnapshot(Snapshot snapshot);


    /**
     * Add the google group for the snapshot readers to the BQ snapshot
     *
     * @param snapshot snapshot metadata
     * @param readersEmail email address for readers group (as returned by SAM)
     */
    void addReaderGroupToSnapshot(Snapshot snapshot, String readersEmail);

    void grantReadAccessToDataset(Dataset dataset, List<String> readerEmails);

    /**
     * Checks to see if a dataset exists
     * @param dataset
     * @return true if the dataset exists, false otherwise
     */
    boolean datasetExists(Dataset dataset);

    /**
     * Checks to see if a table within a dataset exists
     * @param dataset
     * @param tableName
     * @return true if the table exists, false otherwise
     */
    boolean tableExists(Dataset dataset, String tableName);

    /**
     * Checks to see if a snapshot exists
     * @param snapshot
     * @return true if the snapshot exists, false otherwise
     */
    boolean snapshotExists(Snapshot snapshot);

}

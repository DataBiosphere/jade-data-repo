package bio.terra.pdao;

import bio.terra.metadata.DataSnapshot;
import bio.terra.metadata.DataSnapshotSource;
import bio.terra.metadata.DrDataset;
import bio.terra.metadata.RowIdMatch;

import java.util.List;

/**
 * In the long term, we want to make the primary data store be pluggable, supporting different implementations.
 * This interface documents what that pluggable interface might look like. Of course, we never know until we
 * build another one.
 *
 * We use the term "container" for the object that contains a set of tables. In BigQuery, this would be a
 * BigQuery dataset. In Postgresql, it would probably be a schema.
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
     * Check to see if a dataset exists
     */
    boolean datasetExists(String datasetName);

    /**
     * Create the container and tables for a dataset.
     * BigQuery: container is a BigQuery dataset
     *
     * @param dataset
     */
    void createDataset(DrDataset dataset);

    /**
     * Delete the dataset. All tables within the container and the container are deleted
     *
     * @param dataset
     * @return true if the dataset was deleted; false if it was not found; throw on other errors
     */
    boolean deleteDataset(DrDataset dataset);

    /**
     * Check to see if a dataSnapshot exists
     */
    boolean dataSnapshotExists(String dataSnapshotName);

    /**
     * Given inputs from one asset, compute the row ids from the input values. The
     * returned structure provides a list suitable to pass into createDataSnapshot and
     * information to return meaningful errors for mismatched input values.
     *
     * @param dataSnapshot
     * @param source - source in the dataSnapshot we are mapping
     * @param inputValues
     * @return RowIdMatch
     */
    RowIdMatch mapValuesToRows(DataSnapshot dataSnapshot,
                               DataSnapshotSource source,
                               List<String> inputValues);

    /**
     * Create the container, tables and views for a dataSnapshot.
     * BigQuery: container is a BigQuery dataSnapshot
     * @param dataSnapshot
     * @param rowIds - row ids for the root table
     */
    void createDataSnapshot(DataSnapshot dataSnapshot, List<String> rowIds);

    /**
     * Delete the dataSnapshot. All tables within the container and the container are deleted
     *
     * @param dataSnapshot
     * @return true if the dataSnapshot was deleted; false if it was not found; throw on other errors
     */
    boolean deleteDataSnapshot(DataSnapshot dataSnapshot);


    /**
     * Add the google group for the dataSnapshot readers to the BQ dataset
     *
     * @param bqDatasetId bigquery dataset name
     * @param readersEmail email address for readers group (as returned by SAM)
     */
    void addReaderGroupToDataSnapshot(String bqDatasetId, String readersEmail);

    /**
     * Update the athorized views on the dataset to include the tables in the dataSnapshot
     * @param dataSnapshotName
     * @param datasetName
     * @param tableNames
     */
    void authorizeDataSnapshotViewsForDataset(String dataSnapshotName, String datasetName, List<String> tableNames);

    /**
     * Remove the athorized views for the dataSnapshot from the dataset
     * @param dataSnapshotName
     * @param datasetName
     * @param tableNames
     */
    void removeDataSnapshotAuthorizationFromDataset(String dataSnapshotName, String datasetName,
                                                    List<String> tableNames);

}

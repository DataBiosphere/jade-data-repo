package bio.terra.pdao;

import bio.terra.metadata.Dataset;
import bio.terra.metadata.DatasetSource;
import bio.terra.metadata.RowIdMatch;
import bio.terra.metadata.Study;

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
 *         <li>studyExists - validate that the study does not exist before calling create</li>
 *         <li>createStudy - if it finds the study, it deletes it and recreates. That covers the case where the
 *         create operation is interrupted by a service failure and restarts.</li>
 *     </ol>
 *     <li>undo</li>
 *     <ol>
 *         <li>deleteStudy - called on undo; does not fail if the study has already been deleted</li>
 *     </ol>
 * </ul>
 *
 * All implementation-specific exception should be caught by the implementation and wrapped
 * in PdaoException.
 */
public interface PrimaryDataAccess {

    /**
     * Check to see if a study exists
     */
    boolean studyExists(String studyName);

    /**
     * Create the container and tables for a study.
     * BigQuery: container is a BigQuery dataset
     *
     * @param study
     */
    void createStudy(Study study);

    /**
     * Delete the study. All tables within the container and the container are deleted
     *
     * @param study
     * @return true if the study was deleted; false if it was not found; throw on other errors
     */
    boolean deleteStudy(Study study);

    /**
     * Check to see if a dataset exists
     */
    boolean datasetExists(String datasetName);

    /**
     * Given inputs from one asset, compute the row ids from the input values. The
     * returned structure provides a list suitable to pass into createDataset and
     * information to return meaningful errors for mismatched input values.
     *
     * @param dataset
     * @param source - source in the dataset we are mapping
     * @param inputValues
     * @return RowIdMatch
     */
    RowIdMatch mapValuesToRows(bio.terra.metadata.Dataset dataset,
                                      DatasetSource source,
                                      List<String> inputValues);

    /**
     * Create the container, tables and views for a dataset.
     * BigQuery: container is a BigQuery dataset
     *
     * @param dataset
     * @param rowIds - row ids for the root table
     */
    void createDataset(Dataset dataset, List<String> rowIds);

    /**
     * Delete the dataset. All tables within the container and the container are deleted
     *
     * @param dataset
     * @return true if the dataset was deleted; false if it was not found; throw on other errors
     */
    boolean deleteDataset(Dataset dataset);


}

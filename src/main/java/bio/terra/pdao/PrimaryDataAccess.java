package bio.terra.pdao;

import bio.terra.metadata.Dataset;
import bio.terra.metadata.Study;

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
 * All implementation-specific exceptions should be caught by the implementation and wrapped
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
     * Create the container, tables and views for a dataset.
     * BigQuery: container is a BigQuery dataset
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


}

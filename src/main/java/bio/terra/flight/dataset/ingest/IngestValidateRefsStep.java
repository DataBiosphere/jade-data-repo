package bio.terra.flight.dataset.ingest;

import bio.terra.dao.DrDatasetDao;
import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.flight.exception.InvalidFileRefException;
import bio.terra.metadata.Column;
import bio.terra.metadata.FSObjectType;
import bio.terra.metadata.DrDataset;
import bio.terra.metadata.Table;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.List;

public class IngestValidateRefsStep implements Step {
    private static int MAX_ERROR_REF_IDS = 20;

    private DrDatasetDao datasetDao;
    private BigQueryPdao bigQueryPdao;
    private FireStoreFileDao fileDao;

    public IngestValidateRefsStep(DrDatasetDao datasetDao, BigQueryPdao bigQueryPdao, FireStoreFileDao fileDao) {
        this.datasetDao = datasetDao;
        this.bigQueryPdao = bigQueryPdao;
        this.fileDao = fileDao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        DrDataset dataset = IngestUtils.getDataset(context, datasetDao);
        Table table = IngestUtils.getDatasetTable(context, dataset);
        String stagingTableName = IngestUtils.getStagingTableName(context);

        // For each fileref column, scan the staging table and build an array of file ids
        // Then probe the file system tables to validate that the file exists and is part
        // of this dataset. We check all ids and return one complete error.

        List<String> invalidRefIds = new ArrayList<>();
        for (Column column : table.getColumns()) {
            if (StringUtils.equalsIgnoreCase(column.getType(), "FILEREF")) {
                List<String> refIdArray = bigQueryPdao.getRefIds(dataset.getName(), stagingTableName, column);
                List<String> badRefIds =
                    fileDao.validateRefIds(dataset.getId().toString(), refIdArray, FSObjectType.FILE);
                if (badRefIds != null) {
                    invalidRefIds.addAll(badRefIds);
                }
            }
        }

        int invalidIdCount = invalidRefIds.size();
        if (invalidIdCount != 0) {
            // Made a string buffer to appease findbugs; it saw + in the loop and said "bad!"
            StringBuffer errorMessage = new StringBuffer("Invalid file ids found during ingest (");

            List<String> errorDetails = new ArrayList<>();
            int count = 0;
            for (String badId : invalidRefIds) {
                errorDetails.add(badId);
                count++;
                if (count > MAX_ERROR_REF_IDS) {
                    errorMessage.append(MAX_ERROR_REF_IDS + "out of ");
                    break;
                }
            }
            errorMessage.append(invalidIdCount + " returned in details)");
            throw new InvalidFileRefException(errorMessage.toString(), errorDetails);
        }

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // The update will update row ids that are null, so it can be restarted on failure.
        return StepResult.getStepResultSuccess();
    }
}

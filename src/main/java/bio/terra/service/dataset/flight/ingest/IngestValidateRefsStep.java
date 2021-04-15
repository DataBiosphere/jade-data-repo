package bio.terra.service.dataset.flight.ingest;

import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.dataset.exception.InvalidFileRefException;
import bio.terra.common.Column;
import bio.terra.service.dataset.Dataset;
import bio.terra.common.Table;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.model.TableDataType;
import java.util.ArrayList;
import java.util.List;

public class IngestValidateRefsStep implements Step {
    private static final int MAX_ERROR_REF_IDS = 20;

    private final DatasetService datasetService;
    private final BigQueryPdao bigQueryPdao;
    private final FireStoreDao fileDao;

    public IngestValidateRefsStep(DatasetService datasetService,
                                  BigQueryPdao bigQueryPdao,
                                  FireStoreDao fileDao) {
        this.datasetService = datasetService;
        this.bigQueryPdao = bigQueryPdao;
        this.fileDao = fileDao;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        Dataset dataset = IngestUtils.getDataset(context, datasetService);
        Table table = IngestUtils.getDatasetTable(context, dataset);
        String stagingTableName = IngestUtils.getStagingTableName(context);

        // For each fileref column, scan the staging table and build an array of file ids
        // Then probe the file system to validate that the file exists and is part
        // of this dataset. We check all ids and return one complete error.

        List<String> invalidRefIds = new ArrayList<>();
        for (Column column : table.getColumns()) {
            if (column.getType() == TableDataType.FILEREF) {
                List<String> refIdArray = bigQueryPdao.getRefIds(dataset, stagingTableName, column);
                List<String> badRefIds = fileDao.validateRefIds(dataset, refIdArray);
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

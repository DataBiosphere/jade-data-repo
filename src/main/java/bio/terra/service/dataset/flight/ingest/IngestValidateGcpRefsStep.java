package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.common.Table;
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.ArrayList;
import java.util.List;

public class IngestValidateGcpRefsStep extends IngestValidateRefsStep {

  private final DatasetService datasetService;
  private final BigQueryPdao bigQueryPdao;
  private final FireStoreDao fileDao;

  public IngestValidateGcpRefsStep(
      DatasetService datasetService, BigQueryPdao bigQueryPdao, FireStoreDao fileDao) {
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

    return handleInvalidRefs(invalidRefIds);
  }
}

package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.common.Table;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class IngestValidateGcpRefsStep extends IngestValidateRefsStep {

  private final DatasetService datasetService;
  private final BigQueryDatasetPdao bigQueryDatasetPdao;
  private final FireStoreDao fileDao;

  public IngestValidateGcpRefsStep(
      DatasetService datasetService,
      BigQueryDatasetPdao bigQueryDatasetPdao,
      FireStoreDao fileDao) {
    this.datasetService = datasetService;
    this.bigQueryDatasetPdao = bigQueryDatasetPdao;
    this.fileDao = fileDao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    Table table = IngestUtils.getDatasetTable(context, dataset);
    String stagingTableName = IngestUtils.getStagingTableName(context);
//    var storageAuthInfo =
//        workingMap.get(CommonMapKeys.DATASET_STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);

    //var tableServiceClient = azureAuthService.getTableServiceClient(storageAuthInfo);
    //boolean checkForFilesOnAzure = dataset.gcpTabualrData() & dataset.getCloudPlatform() == Azure) {

    // For each fileref column, scan the staging table and build an array of file ids
    // Then probe the file system to validate that the file exists and is part
    // of this dataset. We check all ids and return one complete error.

    Set<InvalidRefId> invalidRefIds = new HashSet<>();
    for (Column column : table.getColumns()) {
      if (column.isFileOrDirRef()) {
        List<String> refIdArray = bigQueryDatasetPdao.getRefIds(dataset, stagingTableName, column);
        // LIst<String> badRefIds;
        // if checkForFilesOnAzure {
        // badRefIds = tableDao.validateRefIds(tableServiceClient, dataset.getId(), refIdArray)
        // else
        List<String> badRefIds = fileDao.validateRefIds(dataset, refIdArray);
        badRefIds.forEach(id -> invalidRefIds.add(new InvalidRefId(id, column.getName())));
      }
    }

    return handleInvalidRefs(invalidRefIds);
  }
}

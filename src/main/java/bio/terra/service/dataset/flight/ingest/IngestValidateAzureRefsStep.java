package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.common.Table;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.tables.TableDirectoryDao;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public record IngestValidateAzureRefsStep(
    AzureAuthService azureAuthService,
    DatasetService datasetService,
    AzureSynapsePdao azureSynapsePdao,
    TableDirectoryDao tableDirectoryDao)
    implements IngestValidateRefsStep {
  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    var workingMap = context.getWorkingMap();
    var dataset = IngestUtils.getDataset(context, datasetService);

    var storageAuthInfo =
        workingMap.get(CommonMapKeys.DATASET_STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);

    var tableServiceClient = azureAuthService.getTableServiceClient(storageAuthInfo);
    Table table = IngestUtils.getDatasetTable(context, dataset);
    var tableName = IngestUtils.getSynapseIngestTableName(context.getFlightId());

    // For each fileref column, scan the staging table and build an array of file ids
    // Then probe the file system to validate that the file exists and is part
    // of this dataset. We check all ids and return one complete error.

    Set<InvalidRefId> invalidRefIds =
        table.getColumns().stream()
            .map(Column::toSynapseColumn)
            .filter(Column::isFileOrDirRef)
            .flatMap(
                column -> {
                  List<String> refIdArray =
                      azureSynapsePdao.getRefIds(tableName, column, dataset.getCollectionType());
                  return tableDirectoryDao.validateRefIds(tableServiceClient, refIdArray).stream()
                      .map(id -> new InvalidRefId(id, column.getName()));
                })
            .collect(Collectors.toSet());

    return handleInvalidRefs(invalidRefIds);
  }
}

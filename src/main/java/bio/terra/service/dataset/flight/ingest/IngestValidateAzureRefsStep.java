package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.common.Table;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.tables.TableDirectoryDao;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class IngestValidateAzureRefsStep extends IngestValidateRefsStep {

  private final AzureAuthService azureAuthService;
  private final DatasetService datasetService;
  private final AzureSynapsePdao azureSynapsePdao;
  private final TableDirectoryDao tableDirectoryDao;

  public IngestValidateAzureRefsStep(
      AzureAuthService azureAuthService,
      DatasetService datasetService,
      AzureSynapsePdao azureSynapsePdao,
      TableDirectoryDao tableDirectoryDao) {
    this.azureAuthService = azureAuthService;
    this.datasetService = datasetService;
    this.azureSynapsePdao = azureSynapsePdao;
    this.tableDirectoryDao = tableDirectoryDao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    var workingMap = context.getWorkingMap();
    var dataset = IngestUtils.getDataset(context, datasetService);

    var billingProfile = workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    var storageAccountResource =
        workingMap.get(FileMapKeys.STORAGE_ACCOUNT_INFO, AzureStorageAccountResource.class);

    var tableServiceClient =
        azureAuthService.getTableServiceClient(
            billingProfile.getSubscriptionId(),
            storageAccountResource.getApplicationResource().getAzureResourceGroupName(),
            storageAccountResource.getName());
    Table table = IngestUtils.getDatasetTable(context, dataset);
    var tableName = IngestUtils.getSynapseTableName(context.getFlightId());

    // For each fileref column, scan the staging table and build an array of file ids
    // Then probe the file system to validate that the file exists and is part
    // of this dataset. We check all ids and return one complete error.

    Set<String> invalidRefIds =
        table.getColumns().stream()
            .map(Column::toSynapseColumn)
            .filter(Column::isFileOrDirRef)
            .flatMap(
                column -> {
                  List<String> refIdArray = azureSynapsePdao.getRefIds(tableName, column);
                  return tableDirectoryDao.validateRefIds(tableServiceClient, refIdArray).stream();
                })
            .collect(Collectors.toSet());

    return handleInvalidRefs(invalidRefIds);
  }
}

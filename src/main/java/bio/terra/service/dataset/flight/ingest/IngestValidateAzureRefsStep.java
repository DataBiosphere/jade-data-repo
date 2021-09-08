package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.common.Table;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.tables.TableDirectoryDao;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.stream.Collectors;

public class IngestValidateAzureRefsStep extends IngestValidateRefsStep {

  private final AzureAuthService azureAuthService;
  private final ResourceService resourceService;
  private final DatasetService datasetService;
  private final AzureSynapsePdao azureSynapsePdao;
  private final TableDirectoryDao tableDirectoryDao;

  public IngestValidateAzureRefsStep(
      ResourceService resourceService,
      AzureAuthService azureAuthService,
      DatasetService datasetService,
      AzureSynapsePdao azureSynapsePdao,
      TableDirectoryDao tableDirectoryDao) {
    this.azureAuthService = azureAuthService;
    this.resourceService = resourceService;
    this.datasetService = datasetService;
    this.azureSynapsePdao = azureSynapsePdao;
    this.tableDirectoryDao = tableDirectoryDao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    var workingMap = context.getWorkingMap();
    var flightId = context.getFlightId();
    var dataset = IngestUtils.getDataset(context, datasetService);

    var billingProfile = workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    var storageAccountResource =
        resourceService.getOrCreateStorageAccount(dataset, billingProfile, flightId);

    var tableServiceClient =
        azureAuthService.getTableServiceClient(billingProfile, storageAccountResource);
    Table table = IngestUtils.getDatasetTable(context, dataset);
    var tableName = IngestUtils.getSynapseTableName(context.getFlightId());

    // For each fileref column, scan the staging table and build an array of file ids
    // Then probe the file system to validate that the file exists and is part
    // of this dataset. We check all ids and return one complete error.

    List<String> invalidRefIds =
        table.getColumns().stream()
            .map(Column::toSynapseColumn)
            .filter(column -> column.getType() == TableDataType.FILEREF)
            .flatMap(
                column -> {
                  List<String> refIdArray = azureSynapsePdao.getRefIds(tableName, column);
                  return tableDirectoryDao.validateRefIds(tableServiceClient, refIdArray).stream();
                })
            .collect(Collectors.toList());

    return handleInvalidRefs(invalidRefIds);
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // The update will update row ids that are null, so it can be restarted on failure.
    return StepResult.getStepResultSuccess();
  }
}

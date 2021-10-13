package bio.terra.service.snapshot.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.tables.TableDependencyDao;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.azure.data.tables.TableServiceClient;
import java.util.List;
import java.util.UUID;

public class CreateSnapshotStorageTableDependenciesStep implements Step {
  private final TableDependencyDao tableDependencyDao;
  private final AzureAuthService azureAuthService;
  private final AzureSynapsePdao azureSynapsePdao;
  private final DatasetService datasetService;

  public CreateSnapshotStorageTableDependenciesStep(
      TableDependencyDao tableDependencyDao,
      AzureAuthService azureAuthService,
      DatasetService datasetService,
      AzureSynapsePdao azureSynapsePdao) {
    this.tableDependencyDao = tableDependencyDao;
    this.azureAuthService = azureAuthService;
    this.azureSynapsePdao = azureSynapsePdao;
    this.datasetService = datasetService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    AzureStorageAuthInfo storageAuthInfo =
        FlightUtils.getContextValue(
            context, FileMapKeys.STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);
    TableServiceClient datasetTableServiceClient =
        azureAuthService.getTableServiceClient(storageAuthInfo);

    Dataset dataset = IngestUtils.getDataset(context, datasetService);
    UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);

    // TODO - place for performance improvement
    List<String> refIds = azureSynapsePdao.getRefIdsForDataset(dataset);
    tableDependencyDao.storeSnapshotFileDependencies(
        datasetTableServiceClient, dataset.getId(), snapshotId, refIds);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {

    return StepResult.getStepResultSuccess();
  }
}

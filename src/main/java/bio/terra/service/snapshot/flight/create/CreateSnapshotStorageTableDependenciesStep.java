package bio.terra.service.snapshot.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.tables.TableDependencyDao;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
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
  private final String datasetName;
  private final SnapshotService snapshotService;

  public CreateSnapshotStorageTableDependenciesStep(
      TableDependencyDao tableDependencyDao,
      AzureAuthService azureAuthService,
      DatasetService datasetService,
      AzureSynapsePdao azureSynapsePdao,
      SnapshotService snapshotService,
      String datasetName) {
    this.tableDependencyDao = tableDependencyDao;
    this.azureAuthService = azureAuthService;
    this.azureSynapsePdao = azureSynapsePdao;
    this.datasetService = datasetService;
    this.snapshotService = snapshotService;
    this.datasetName = datasetName;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    AzureStorageAuthInfo datasetStorageAuthInfo =
        FlightUtils.getContextValue(
            context, CommonMapKeys.DATASET_STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);
    TableServiceClient datasetTableServiceClient =
        azureAuthService.getTableServiceClient(datasetStorageAuthInfo);

    Dataset dataset = datasetService.retrieveByName(datasetName);
    UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
    Snapshot snapshot = snapshotService.retrieve(snapshotId);

    // TODO - place for performance improvement
    List<String> refIds = azureSynapsePdao.getRefIdsForSnapshot(snapshot);
    tableDependencyDao.storeSnapshotFileDependencies(
        datasetTableServiceClient, dataset.getId(), snapshotId, refIds);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {

    return StepResult.getStepResultSuccess();
  }
}

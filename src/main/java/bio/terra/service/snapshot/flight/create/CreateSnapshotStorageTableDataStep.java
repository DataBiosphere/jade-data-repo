package bio.terra.service.snapshot.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.common.azure.StorageTableName;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.azure.data.tables.TableServiceClient;
import java.util.Set;
import java.util.UUID;

public class CreateSnapshotStorageTableDataStep implements Step {
  private final TableDao tableDao;
  private final AzureAuthService azureAuthService;
  private final AzureSynapsePdao azureSynapsePdao;
  private final UUID datasetId;
  private final String datasetName;
  private final SnapshotService snapshotService;
  private final UUID snapshotId;

  public CreateSnapshotStorageTableDataStep(
      TableDao tableDao,
      AzureAuthService azureAuthService,
      AzureSynapsePdao azureSynapsePdao,
      SnapshotService snapshotService,
      UUID datasetId,
      String datasetName,
      UUID snapshotId) {
    this.tableDao = tableDao;
    this.azureAuthService = azureAuthService;
    this.azureSynapsePdao = azureSynapsePdao;
    this.datasetId = datasetId;
    this.datasetName = datasetName;
    this.snapshotService = snapshotService;
    this.snapshotId = snapshotId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    // Dataset
    AzureStorageAuthInfo datasetStorageAuthInfo =
        FlightUtils.getContextValue(
            context, CommonMapKeys.DATASET_STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);
    TableServiceClient datasetTableServiceClient =
        azureAuthService.getTableServiceClient(datasetStorageAuthInfo);
    Snapshot snapshot = snapshotService.retrieve(snapshotId);

    // Snapshot
    AzureStorageAuthInfo snapshotStorageAuthInfo =
        FlightUtils.getContextValue(
            context, CommonMapKeys.SNAPSHOT_STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);
    TableServiceClient snapshotTableServiceClient =
        azureAuthService.getTableServiceClient(snapshotStorageAuthInfo);

    Set<String> refIds = azureSynapsePdao.getRefIdsForSnapshot(snapshot);

    tableDao.addFilesToSnapshot(
        datasetTableServiceClient,
        snapshotTableServiceClient,
        datasetId,
        datasetName,
        snapshot,
        refIds);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    String tableToDelete = StorageTableName.SNAPSHOT.toTableName(snapshotId);

    AzureStorageAuthInfo snapshotStorageAuthInfo =
        FlightUtils.getContextValue(
            context, CommonMapKeys.SNAPSHOT_STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);
    TableServiceClient datasetTableServiceClient =
        azureAuthService.getTableServiceClient(snapshotStorageAuthInfo);
    datasetTableServiceClient.deleteTable(tableToDelete);

    return StepResult.getStepResultSuccess();
  }
}

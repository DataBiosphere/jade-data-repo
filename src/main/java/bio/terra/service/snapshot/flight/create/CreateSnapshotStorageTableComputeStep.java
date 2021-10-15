package bio.terra.service.snapshot.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.azure.data.tables.TableServiceClient;

public class CreateSnapshotStorageTableComputeStep implements Step {

  private SnapshotService snapshotService;
  private SnapshotRequestModel snapshotReq;
  private TableDao tableDao;
  private AzureAuthService azureAuthService;

  public CreateSnapshotStorageTableComputeStep(
      TableDao tableDao,
      SnapshotRequestModel snapshotReq,
      SnapshotService snapshotService,
      AzureAuthService azureAuthService) {
    this.tableDao = tableDao;
    this.snapshotReq = snapshotReq;
    this.snapshotService = snapshotService;
    this.azureAuthService = azureAuthService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    AzureStorageAuthInfo datasetStorageAuthInfo =
        FlightUtils.getContextValue(
            context, CommonMapKeys.DATASET_STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);
    TableServiceClient datasetTableServiceClient =
        azureAuthService.getTableServiceClient(datasetStorageAuthInfo);

    AzureStorageAuthInfo snapshotStorageAuthInfo =
        FlightUtils.getContextValue(
            context, CommonMapKeys.SNAPSHOT_STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);
    TableServiceClient snapshotTableServiceClient =
        azureAuthService.getTableServiceClient(snapshotStorageAuthInfo);

    Snapshot snapshot = snapshotService.retrieveByName(snapshotReq.getName());
    // Compute the size and checksums
    tableDao.snapshotCompute(snapshot, datasetTableServiceClient, snapshotTableServiceClient);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // No undo - if we are undoing all the way, the whole snapshot file system will get
    // torn down.
    return StepResult.getStepResultSuccess();
  }
}

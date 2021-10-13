package bio.terra.service.snapshot.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
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
    FlightMap workingMap = context.getWorkingMap();
    AzureStorageAuthInfo storageAuthInfo =
        FlightUtils.getContextValue(
            context, FileMapKeys.STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);
    // TODO - get snapshot vs. dataset storage account
    TableServiceClient datasetTableServiceClient =
        azureAuthService.getTableServiceClient(storageAuthInfo);
    TableServiceClient snapshotTableServiceClient =
        azureAuthService.getTableServiceClient(storageAuthInfo);

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

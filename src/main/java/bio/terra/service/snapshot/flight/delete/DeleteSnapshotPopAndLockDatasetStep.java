package bio.terra.service.snapshot.flight.delete;

import bio.terra.common.exception.RetryQueryException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;

public class DeleteSnapshotPopAndLockDatasetStep implements Step {

  private final SnapshotService snapshotService;
  private final DatasetService datasetService;
  private final UUID snapshotId;
  private final AuthenticatedUserRequest authenticatedUserRequest;
  private final boolean sharedLock;

  public DeleteSnapshotPopAndLockDatasetStep(
      UUID snapshotId,
      SnapshotService snapshotService,
      DatasetService datasetService,
      AuthenticatedUserRequest authenticatedUserRequest,
      boolean sharedLock) {
    this.snapshotId = snapshotId;
    this.snapshotService = snapshotService;
    this.datasetService = datasetService;
    this.authenticatedUserRequest = authenticatedUserRequest;
    this.sharedLock = sharedLock;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap map = context.getWorkingMap();
    Snapshot snapshot;

    // Confirm that snapshot exists and populate info about snapshot to inform optional steps
    try {
      snapshot = snapshotService.retrieve(snapshotId);
      map.put(SnapshotWorkingMapKeys.SNAPSHOT_EXISTS, true);
      boolean hasGoogleProject =
          snapshot.getProjectResource() != null
              && snapshot.getProjectResource().getGoogleProjectId() != null;
      map.put(SnapshotWorkingMapKeys.SNAPSHOT_HAS_GOOGLE_PROJECT, hasGoogleProject);
      boolean hasAzureStorageAccount = snapshot.getStorageAccountResource() != null;
      map.put(SnapshotWorkingMapKeys.SNAPSHOT_HAS_AZURE_STORAGE_ACCOUNT, hasAzureStorageAccount);
    } catch (SnapshotNotFoundException snapshotNotFoundException) {
      map.put(SnapshotWorkingMapKeys.SNAPSHOT_EXISTS, false);
      map.put(SnapshotWorkingMapKeys.DATASET_EXISTS, false);
      map.put(SnapshotWorkingMapKeys.SNAPSHOT_HAS_GOOGLE_PROJECT, false);
      map.put(SnapshotWorkingMapKeys.SNAPSHOT_HAS_AZURE_STORAGE_ACCOUNT, false);
      return StepResult.getStepResultSuccess();
    }

    // Now we've confirmed the snapshot exists, let's check on the source dataset
    UUID datasetId = snapshot.getSourceDataset().getId();
    map.put(DatasetWorkingMapKeys.DATASET_ID, datasetId);
    boolean datasetExists = true;
    try {
      datasetService.lock(datasetId, context.getFlightId(), sharedLock);
    } catch (DatasetNotFoundException notFoundEx) {
      datasetExists = false;
    } catch (RetryQueryException | DatasetLockException e) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
    }
    map.put(SnapshotWorkingMapKeys.DATASET_EXISTS, datasetExists);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    try {
      UUID sourceDatasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
      datasetService.unlock(sourceDatasetId, context.getFlightId(), sharedLock);
    } catch (DatasetLockException | DatasetNotFoundException ex) {
      // DatasetLockException will be thrown if flight id was not set
      return StepResult.getStepResultSuccess();
    } catch (RetryQueryException e) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
    }
    return StepResult.getStepResultSuccess();
  }
}

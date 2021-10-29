package bio.terra.service.snapshot.flight.delete;

import bio.terra.common.exception.RetryQueryException;
import bio.terra.service.dataset.Dataset;
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
import java.util.List;
import java.util.UUID;

public class DeleteSnapshotPopAndLockDatasetStep implements Step {

  private SnapshotService snapshotService;
  private DatasetService datasetService;
  private UUID snapshotId;
  private boolean sharedLock;

  public DeleteSnapshotPopAndLockDatasetStep(
      UUID snapshotId,
      SnapshotService snapshotService,
      DatasetService datasetService,
      boolean sharedLock) {
    this.snapshotId = snapshotId;
    this.snapshotService = snapshotService;
    this.datasetService = datasetService;
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
    UUID datasetId = snapshot.getFirstSnapshotSource().getId();
    map.put(DatasetWorkingMapKeys.DATASET_ID, datasetId);
    try {
      datasetService.lockDataset(datasetId, context.getFlightId(), sharedLock);
      map.put(SnapshotWorkingMapKeys.DATASET_EXISTS, true);
    } catch (DatasetNotFoundException notFoundEx) {
      map.put(SnapshotWorkingMapKeys.DATASET_EXISTS, false);
    } catch (RetryQueryException | DatasetLockException e) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    FlightMap map = context.getWorkingMap();
    boolean datasetExists = map.get(SnapshotWorkingMapKeys.DATASET_EXISTS, Boolean.class);
    if (datasetExists) {
      List<Dataset> sourceDatasets = snapshotService.getSourceDatasetsFromSnapshotId(snapshotId);
      Dataset sourceDataset = sourceDatasets.get(0);
      UUID datasetId = sourceDataset.getId();
      try {
        datasetService.unlockDataset(datasetId, context.getFlightId(), sharedLock);
      } catch (DatasetLockException | DatasetNotFoundException ex) {
        // DatasetLockException will be thrown if flight id was not set
        return StepResult.getStepResultSuccess();
      } catch (RetryQueryException e) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, e);
      }
    }
    return StepResult.getStepResultSuccess();
  }
}

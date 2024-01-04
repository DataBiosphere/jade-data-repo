package bio.terra.service.dataset.flight.delete;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.exception.FileSystemCorruptException;
import bio.terra.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotSummary;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;

public abstract class DeleteDatasetValidateStep implements Step {
  private SnapshotDao snapshotDao;
  protected FireStoreDependencyDao dependencyDao;
  protected DatasetService datasetService;
  private UUID datasetId;

  public DeleteDatasetValidateStep(
      SnapshotDao snapshotDao,
      FireStoreDependencyDao dependencyDao,
      DatasetService datasetService,
      UUID datasetId) {
    this.snapshotDao = snapshotDao;
    this.dependencyDao = dependencyDao;
    this.datasetService = datasetService;
    this.datasetId = datasetId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    List<SnapshotSummary> snapshots = snapshotDao.retrieveSnapshotsForDataset(datasetId);
    Dataset dataset = datasetService.retrieve(datasetId);

    if (snapshots.size() != 0) {
      throw new ValidationException("Can not delete a dataset being used by snapshots");
    }
    // Sanity check - validate that there are no stray file references. There should be none left
    // if there are no snapshots returned from retrieveSnapshotsForDataset.
    if (hasSnapshotReference(dataset, context)) {
      throw new FileSystemCorruptException(
          "File system has snapshot dependencies; metadata does not");
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // no undo is possible
    return StepResult.getStepResultSuccess();
  }

  abstract boolean hasSnapshotReference(Dataset dataset, FlightContext context)
      throws InterruptedException;
}

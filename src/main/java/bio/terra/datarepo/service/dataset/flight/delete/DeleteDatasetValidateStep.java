package bio.terra.datarepo.service.dataset.flight.delete;

import bio.terra.datarepo.app.controller.exception.ValidationException;
import bio.terra.datarepo.service.dataset.Dataset;
import bio.terra.datarepo.service.dataset.DatasetService;
import bio.terra.datarepo.service.filedata.exception.FileSystemCorruptException;
import bio.terra.datarepo.service.filedata.google.firestore.FireStoreDependencyDao;
import bio.terra.datarepo.service.snapshot.SnapshotDao;
import bio.terra.datarepo.service.snapshot.SnapshotSummary;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;

public class DeleteDatasetValidateStep implements Step {
  private SnapshotDao snapshotDao;
  private FireStoreDependencyDao dependencyDao;
  private DatasetService datasetService;
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
    if (dependencyDao.datasetHasSnapshotReference(dataset)) {
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
}

package bio.terra.service.snapshot.flight.create;

import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.filedata.DrsService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class SnapshotRecordFileIdsStep implements Step {

  private static final Logger logger = LoggerFactory.getLogger(SnapshotRecordFileIdsStep.class);

  private final SnapshotService snapshotService;
  private final DatasetService datasetService;
  private final DrsIdService drsIdService;
  private final DrsService drsService;
  private final UUID snapshotId;

  public SnapshotRecordFileIdsStep(
      SnapshotService snapshotService,
      DatasetService datasetService,
      DrsIdService drsIdService,
      DrsService drsService,
      UUID snapshotId) {
    this.snapshotService = snapshotService;
    this.datasetService = datasetService;
    this.drsIdService = drsIdService;
    this.drsService = drsService;
    this.snapshotId = snapshotId;
  }

  //  abstract List<String> getFileIds(FlightContext context, Dataset dataset, UUID snapshotId)
  //      throws InterruptedException;
  abstract List<String> getFileIds(FlightContext context, Snapshot snapshot)
      throws InterruptedException;

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    List<String> fileIds = getFileIds(context, snapshot);

    logger.info(
        "Inserted {} rows",
        drsService.recordDrsIdToSnapshot(
            snapshotId, fileIds.stream().map(drsIdService::makeDrsId).toList()));

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    logger.info("Deleted {} rows", drsService.deleteDrsIdToSnapshotsBySnapshot(snapshotId));

    return StepResult.getStepResultSuccess();
  }
}

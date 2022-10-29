package bio.terra.service.snapshot.flight.duos;

import static bio.terra.service.snapshot.flight.duos.SnapshotDuosFlightUtils.getFirecloudGroup;

import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.model.SnapshotLinkDuosDatasetResponse;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.Optional;
import java.util.UUID;

public class UpdateSnapshotDuosFirecloudGroupIdStep implements Step {

  private final SnapshotDao snapshotDao;
  private final UUID snapshotId;
  private final DuosFirecloudGroupModel duosFirecloudGroupPrev;

  public UpdateSnapshotDuosFirecloudGroupIdStep(
      SnapshotDao snapshotDao, UUID snapshotId, DuosFirecloudGroupModel duosFirecloudGroupPrev) {
    this.snapshotDao = snapshotDao;
    this.snapshotId = snapshotId;
    this.duosFirecloudGroupPrev = duosFirecloudGroupPrev;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    DuosFirecloudGroupModel duosFirecloudGroup = getFirecloudGroup(context);
    UUID duosFirecloudGroupId = getDuosFirecloudGroupId(duosFirecloudGroup);
    snapshotDao.updateDuosFirecloudGroupId(snapshotId, duosFirecloudGroupId);

    SnapshotLinkDuosDatasetResponse response =
        new SnapshotLinkDuosDatasetResponse()
            .linked(duosFirecloudGroup)
            .unlinked(duosFirecloudGroupPrev);
    context.getWorkingMap().put(JobMapKeys.RESPONSE.getKeyName(), response);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    UUID duosFirecloudGroupIdPrev = getDuosFirecloudGroupId(duosFirecloudGroupPrev);
    snapshotDao.updateDuosFirecloudGroupId(snapshotId, duosFirecloudGroupIdPrev);
    return StepResult.getStepResultSuccess();
  }

  private UUID getDuosFirecloudGroupId(DuosFirecloudGroupModel duosFirecloudGroup) {
    return Optional.ofNullable(duosFirecloudGroup).map(DuosFirecloudGroupModel::getId).orElse(null);
  }
}

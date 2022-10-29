package bio.terra.service.snapshot.flight.duos;

import static bio.terra.service.snapshot.flight.duos.SnapshotDuosFlightUtils.getFirecloudGroup;

import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.service.duos.DuosDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class RecordDuosFirecloudGroupStep implements Step {

  private final DuosDao duosDao;

  public RecordDuosFirecloudGroupStep(DuosDao duosDao) {
    this.duosDao = duosDao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    UUID duosFirecloudGroupId = duosDao.insertFirecloudGroup(getFirecloudGroup(context));
    DuosFirecloudGroupModel retrieved = duosDao.retrieveFirecloudGroup(duosFirecloudGroupId);

    // When inserting a new record, DuosDao and the DB generate additional metadata
    // (ex. creation time) so we'll retrieve the record and rewrite it in the working map.
    context.getWorkingMap().put(SnapshotDuosMapKeys.FIRECLOUD_GROUP, retrieved);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    UUID duosFirecloudGroupId = getFirecloudGroup(context).getId();
    if (duosFirecloudGroupId != null) {
      duosDao.deleteFirecloudGroup(duosFirecloudGroupId);
    }
    return StepResult.getStepResultSuccess();
  }
}

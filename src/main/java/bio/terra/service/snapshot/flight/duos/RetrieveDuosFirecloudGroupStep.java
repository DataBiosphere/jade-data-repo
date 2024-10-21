package bio.terra.service.snapshot.flight.duos;

import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.service.duos.DuosDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;

public class RetrieveDuosFirecloudGroupStep implements Step {

  private final DuosDao duosDao;
  private final String duosId;

  public RetrieveDuosFirecloudGroupStep(DuosDao duosDao, String duosId) {
    this.duosDao = duosDao;
    this.duosId = duosId;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    DuosFirecloudGroupModel retrieved = duosDao.retrieveFirecloudGroupByDuosId(duosId);

    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP, retrieved);
    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP_RETRIEVED, (retrieved != null));

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}

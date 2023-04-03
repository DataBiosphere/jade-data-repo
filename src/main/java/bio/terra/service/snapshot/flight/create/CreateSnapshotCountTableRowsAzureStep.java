package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.HashMap;
import java.util.Map;

public record CreateSnapshotCountTableRowsAzureStep(
    AzureSynapsePdao azureSynapsePdao, SnapshotDao snapshotDao, SnapshotRequestModel snapshotReq)
    implements Step {

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    FlightMap workingMap = flightContext.getWorkingMap();
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    Map<String, Long> tableRowCounts =
        workingMap.get(SnapshotWorkingMapKeys.TABLE_ROW_COUNT_MAP, HashMap.class);
    snapshotDao.updateSnapshotTableRowCounts(snapshot, tableRowCounts);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    // nothing to do here
    return StepResult.getStepResultSuccess();
  }
}

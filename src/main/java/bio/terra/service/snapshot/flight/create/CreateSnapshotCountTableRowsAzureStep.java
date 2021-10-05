package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.HashMap;
import java.util.Map;

public class CreateSnapshotCountTableRowsAzureStep implements Step {

  private final AzureSynapsePdao azureSynapsePdao;
  private final SnapshotDao snapshotDao;
  private final SnapshotRequestModel snapshotReq;

  public CreateSnapshotCountTableRowsAzureStep(
      AzureSynapsePdao azureSynapsePdao,
      SnapshotDao snapshotDao,
      SnapshotRequestModel snapshotReq) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.snapshotDao = snapshotDao;
    this.snapshotReq = snapshotReq;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    // TODO - provide new get row counts method
    // potentially can get from CreateSnapshotParquetFilesAzureStep
    Map<String, Long> tableRowCounts =
        new HashMap<>(); // azureSynapsePdao.getSnapshotTableRowCounts(snapshot);
    snapshotDao.updateSnapshotTableRowCounts(snapshot, tableRowCounts);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    // nothing to do here
    return StepResult.getStepResultSuccess();
  }
}

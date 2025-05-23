package bio.terra.service.dataset.flight.inheritsteward;

import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.Objects;
import java.util.UUID;

public record SetAuthTabularAclStep(
    BigQuerySnapshotPdao bigQuerySnapshotPdao,
    SnapshotService snapshotService,
    List<String> datasetPolicyEmails,
    boolean inheritSteward)
    implements Step {

  /**
   * Either set or remove the custodian email from the READER ACL of the snapshot's bigquery
   * database.
   *
   * <p>If inheritSteward is true, we use the dataset custodian's proxy email group to grant access
   * to the snapshot's data. When inheritSteward is false, users must be directly added to the
   * snapshot's Sam policies, which will then grant them access to the snapshot's data.
   */
  private void setAuth(FlightContext context, boolean inheritSteward) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    List<UUID> snapshotIds =
        workingMap.get(DatasetWorkingMapKeys.SNAPSHOT_IDS, List.class);
    for (var snapshotId : Objects.requireNonNull(snapshotIds)) {
      Snapshot snapshot = snapshotService.retrieve(snapshotId);
      if (inheritSteward) {
        bigQuerySnapshotPdao.grantReadAccessToSnapshot(snapshot, datasetPolicyEmails);
      } else {
        bigQuerySnapshotPdao.revokeReadAccessToSnapshot(snapshot, datasetPolicyEmails);
      }
    }
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    setAuth(context, inheritSteward);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    setAuth(context, !inheritSteward);
    return StepResult.getStepResultSuccess();
  }
}

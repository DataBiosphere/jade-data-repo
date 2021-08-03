package bio.terra.service.snapshot.flight.create;

import static bio.terra.service.configuration.ConfigEnum.SNAPSHOT_GRANT_ACCESS_FAULT;

import bio.terra.common.FlightUtils;
import bio.terra.common.exception.PdaoException;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.iam.IamRole;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class SnapshotAuthzTabularAclStep implements Step {
  private final BigQueryPdao bigQueryPdao;
  private final SnapshotService snapshotService;
  private final ConfigurationService configService;

  public SnapshotAuthzTabularAclStep(
      BigQueryPdao bigQueryPdao,
      SnapshotService snapshotService,
      ConfigurationService configService) {
    this.bigQueryPdao = bigQueryPdao;
    this.snapshotService = snapshotService;
    this.configService = configService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
    Snapshot snapshot = snapshotService.retrieve(snapshotId);

    Map<IamRole, String> policies = workingMap.get(SnapshotWorkingMapKeys.POLICY_MAP, Map.class);
    // Build the list of the policy emails that should have read access to the big query dataset
    List<String> emails = new ArrayList<>();
    emails.add(policies.get(IamRole.STEWARD));
    emails.add(policies.get(IamRole.READER));

    try {
      if (configService.testInsertFault(SNAPSHOT_GRANT_ACCESS_FAULT)) {
        throw new BigQueryException(
            400,
            "IAM setPolicy fake failure",
            new BigQueryError("invalid", "fake", "IAM setPolicy fake failure"));
      }
      bigQueryPdao.grantReadAccessToSnapshot(snapshot, emails);
    } catch (BigQueryException ex) {
      if (FlightUtils.isBigQueryIamPropagationError(ex)) {
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
      }
      throw new PdaoException("Caught BQ exception while granting read access to snapshot", ex);
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // We do not try to undo the ACL set, because we expect the entire BQ dataset to get deleted,
    // taking the ACLs with it if we are on this undo path.
    return StepResult.getStepResultSuccess();
  }
}

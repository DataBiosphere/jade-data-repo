package bio.terra.service.snapshot.flight.create;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SnapshotAuthzTabularAclStepTest {
  @Mock private BigQuerySnapshotPdao bigQuerySnapshotPdao;
  @Mock private SnapshotService snapshotService;
  @Mock private ConfigurationService configService;
  @Mock private IamService iamService;
  @Mock private FlightContext flightContext;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final Snapshot SNAPSHOT = new Snapshot().id(UUID.randomUUID());
  private static final Dataset SOURCE_DATASET = new Dataset().id(UUID.randomUUID());

  private SnapshotAuthzTabularAclStep step;
  private FlightMap inputMap;

  @BeforeEach
  void beforeEach() {
    FlightMap workingMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    inputMap = new FlightMap();
    when(flightContext.getInputParameters()).thenReturn(inputMap);

    var policyMap = new EnumMap<>(IamRole.class);
    policyMap.put(IamRole.STEWARD, "steward");
    policyMap.put(IamRole.READER, "reader");
    workingMap.put(SnapshotWorkingMapKeys.POLICY_MAP, policyMap);

    when(snapshotService.retrieve(SNAPSHOT.getId())).thenReturn(SNAPSHOT);

    step =
        new SnapshotAuthzTabularAclStep(
            bigQuerySnapshotPdao,
            snapshotService,
            configService,
            iamService,
            SNAPSHOT.getId(),
            TEST_USER,
            SOURCE_DATASET);
  }

  @Test
  void doStep() throws Exception {
    assertThat(step.doStep(flightContext), is(StepResult.getStepResultSuccess()));
    verify(bigQuerySnapshotPdao).grantReadAccessToSnapshot(SNAPSHOT, List.of("steward", "reader"));
    verifyNoInteractions(iamService);
  }

  @Test
  void doStepInheritEnabled() throws Exception {
    inputMap.put(SnapshotWorkingMapKeys.SNAPSHOT_INHERIT_STEWARD_ENABLED, true);
    when(iamService.retrievePolicyEmails(
            TEST_USER, IamResourceType.DATASET, SOURCE_DATASET.getId()))
        .thenReturn(Map.of(IamRole.CUSTODIAN, "custodian"));
    assertThat(step.doStep(flightContext), is(StepResult.getStepResultSuccess()));
    verify(bigQuerySnapshotPdao)
        .grantReadAccessToSnapshot(SNAPSHOT, List.of("steward", "reader", "custodian"));
  }

  @Test
  void doStepDatabaseRetry() throws Exception {
    doThrow(
            new BigQueryException(
                500,
                "IAM setPolicy",
                new BigQueryError("invalid", "fake", "IAM setPolicy fake failure")))
        .when(bigQuerySnapshotPdao)
        .grantReadAccessToSnapshot(SNAPSHOT, List.of("steward", "reader"));
    assertThat(
        step.doStep(flightContext).getStepStatus(), is(StepStatus.STEP_RESULT_FAILURE_RETRY));
  }

  @Test
  void undoStep() {
    reset(snapshotService, flightContext);
    assertThat(step.undoStep(flightContext), is(StepResult.getStepResultSuccess()));
    verifyNoInteractions(snapshotService, bigQuerySnapshotPdao, iamService, flightContext);
  }
}

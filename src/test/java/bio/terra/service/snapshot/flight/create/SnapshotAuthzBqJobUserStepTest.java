package bio.terra.service.snapshot.flight.create;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
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
class SnapshotAuthzBqJobUserStepTest {
  @Mock private SnapshotService snapshotService;
  @Mock private ResourceService resourceService;
  @Mock private IamService iamService;
  @Mock private FlightContext flightContext;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final String SNAPSHOT_NAME = "snapshotName";
  private static final String GOOGLE_PROJECT_ID = "google project id";
  private static final Snapshot SNAPSHOT =
      new Snapshot()
          .projectResource(new GoogleProjectResource().googleProjectId(GOOGLE_PROJECT_ID));
  private static final Dataset SOURCE_DATASET = new Dataset().id(UUID.randomUUID());

  private SnapshotAuthzBqJobUserStep step;
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
    when(snapshotService.retrieveByName(SNAPSHOT_NAME)).thenReturn(SNAPSHOT);

    step =
        new SnapshotAuthzBqJobUserStep(
            snapshotService, resourceService, iamService, TEST_USER, SNAPSHOT_NAME, SOURCE_DATASET);
  }

  @Test
  void doStep() throws Exception {
    step.doStep(flightContext);
    verify(resourceService).grantPoliciesBqJobUser(GOOGLE_PROJECT_ID, List.of("steward"));
    verify(resourceService).grantPoliciesBqJobUser(GOOGLE_PROJECT_ID, List.of("reader"));
    verifyNoMoreInteractions(resourceService);
    verifyNoInteractions(iamService);
  }

  @Test
  void doStepInheritEnabled() throws Exception {
    inputMap.put(SnapshotWorkingMapKeys.SNAPSHOT_INHERIT_STEWARD_ENABLED, true);
    when(iamService.retrievePolicyEmails(
            TEST_USER, IamResourceType.DATASET, SOURCE_DATASET.getId()))
        .thenReturn(Map.of(IamRole.CUSTODIAN, "custodian"));
    step.doStep(flightContext);
    verify(resourceService).grantPoliciesBqJobUser(GOOGLE_PROJECT_ID, List.of("steward"));
    verify(resourceService).grantPoliciesBqJobUser(GOOGLE_PROJECT_ID, List.of("reader"));
    verify(resourceService).grantPoliciesBqJobUser(GOOGLE_PROJECT_ID, List.of("custodian"));
  }

  @Test
  void undoStep() throws Exception {
    reset(snapshotService, flightContext);
    assertThat(step.undoStep(flightContext), is(StepResult.getStepResultSuccess()));
    verifyNoInteractions(snapshotService, resourceService, iamService, flightContext);
  }
}

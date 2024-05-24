package bio.terra.service.snapshot.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.fixtures.DuosFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestModelPolicies;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.create.SnapshotAuthzIamStep;
import bio.terra.service.snapshot.flight.duos.SnapshotDuosMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SnapshotAuthzIamStepTest {
  @Mock private IamService iamService;
  @Mock private SnapshotService snapshotService;
  @Mock private FlightContext flightContext;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final String DUOS_ID = "DUOS-123456";
  private static final DuosFirecloudGroupModel DUOS_FIRECLOUD_GROUP =
      DuosFixtures.createDbFirecloudGroup(DUOS_ID);

  private SnapshotAuthzIamStep step;
  private FlightMap workingMap;
  private SnapshotRequestModel snapshotRequestModel;

  @BeforeEach
  void setup() {
    workingMap = new FlightMap();
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_ID, SNAPSHOT_ID);
    snapshotRequestModel = new SnapshotRequestModel();
    when(iamService.deriveSnapshotPolicies(snapshotRequestModel))
        .thenReturn(new SnapshotRequestModelPolicies().readers(new ArrayList<>()));
  }

  @Test
  void testDoAndUndoStep() throws InterruptedException {
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    step =
        new SnapshotAuthzIamStep(
            iamService, snapshotService, snapshotRequestModel, TEST_USER, SNAPSHOT_ID);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));

    ArgumentCaptor<SnapshotRequestModelPolicies> argument =
        ArgumentCaptor.forClass(SnapshotRequestModelPolicies.class);
    verify(iamService).createSnapshotResource(eq(TEST_USER), eq(SNAPSHOT_ID), argument.capture());
    List<String> readers = argument.getValue().getReaders();
    assertFalse(readers.contains(DUOS_FIRECLOUD_GROUP.getFirecloudGroupEmail()));

    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(iamService).deleteSnapshotResource(TEST_USER, SNAPSHOT_ID);
  }

  @Test
  void testDoAndUndoStepWithDUOS() throws InterruptedException {
    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP, DUOS_FIRECLOUD_GROUP);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);

    snapshotRequestModel.duosId(DUOS_ID);
    step =
        new SnapshotAuthzIamStep(
            iamService, snapshotService, snapshotRequestModel, TEST_USER, SNAPSHOT_ID);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));

    SnapshotRequestModelPolicies policies = iamService.deriveSnapshotPolicies(snapshotRequestModel);
    policies.addReadersItem(DUOS_FIRECLOUD_GROUP.getFirecloudGroupEmail());
    ArgumentCaptor<SnapshotRequestModelPolicies> argument =
        ArgumentCaptor.forClass(SnapshotRequestModelPolicies.class);
    verify(iamService).createSnapshotResource(eq(TEST_USER), eq(SNAPSHOT_ID), argument.capture());
    List<String> readers = argument.getValue().getReaders();
    assertTrue(readers.contains(DUOS_FIRECLOUD_GROUP.getFirecloudGroupEmail()));

    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(iamService).deleteSnapshotResource(TEST_USER, SNAPSHOT_ID);
  }
}

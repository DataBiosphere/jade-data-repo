package bio.terra.service.policy.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.exception.FeatureNotImplementedException;
import bio.terra.policy.model.TpsPolicyInput;
import bio.terra.policy.model.TpsPolicyInputs;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.policy.exception.PolicyConflictException;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshot.flight.create.CreateSnapshotPolicyStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag("bio.terra.common.category.Unit")
public class CreateSnapshotPolicyStepTest {
  @Mock private PolicyService policyService;
  @Mock private FlightContext flightContext;

  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final TpsPolicyInput protectedDataPolicy =
      PolicyService.getProtectedDataPolicyInput();
  private static final TpsPolicyInputs policies =
      new TpsPolicyInputs().addInputsItem(protectedDataPolicy);

  private void mockFlightMap() {
    FlightMap workingMap = new FlightMap();
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_ID, SNAPSHOT_ID);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  void testProtectedDataPolicyDoUndo() throws Exception {
    mockFlightMap();
    CreateSnapshotPolicyStep step = new CreateSnapshotPolicyStep(policyService, true);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(policyService).createSnapshotPao(SNAPSHOT_ID, policies);
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(policyService).deletePao(SNAPSHOT_ID);
  }

  @Test
  void testProtectedDataPolicyAlreadyExists() throws Exception {
    mockFlightMap();
    CreateSnapshotPolicyStep step = new CreateSnapshotPolicyStep(policyService, true);
    var exception = new PolicyConflictException("Policy access object already exists");
    doThrow(exception).when(policyService).createSnapshotPao(SNAPSHOT_ID, policies);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(policyService).createSnapshotPao(SNAPSHOT_ID, policies);
  }

  @Test
  void testNoPolicyDoUndo() throws Exception {
    CreateSnapshotPolicyStep step = new CreateSnapshotPolicyStep(policyService, false);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(policyService, never()).createSnapshotPao(SNAPSHOT_ID, policies);
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(policyService, never()).deletePao(SNAPSHOT_ID);
  }

  @Test
  void testCreatePolicyServiceNotEnabled() throws Exception {
    mockFlightMap();
    CreateSnapshotPolicyStep step = new CreateSnapshotPolicyStep(policyService, true);
    var exception = new FeatureNotImplementedException("Policy service is not enabled");
    doThrow(exception).when(policyService).createSnapshotPao(SNAPSHOT_ID, policies);
    doThrow(exception).when(policyService).deletePao(SNAPSHOT_ID);

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(policyService).createSnapshotPao(SNAPSHOT_ID, policies);

    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(policyService).deletePao(SNAPSHOT_ID);
  }
}

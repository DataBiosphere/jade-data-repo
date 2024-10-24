package bio.terra.service.policy.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

import bio.terra.common.category.Unit;
import bio.terra.policy.model.TpsObjectType;
import bio.terra.policy.model.TpsPolicyInput;
import bio.terra.policy.model.TpsPolicyInputs;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.snapshot.flight.create.CreateSnapshotPolicyStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class CreateSnapshotPolicyStepTest {
  @Mock private PolicyService policyService;
  @Mock private FlightContext flightContext;

  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final TpsPolicyInput protectedDataPolicy =
      PolicyService.getProtectedDataPolicyInput();
  private static final TpsPolicyInputs policies =
      new TpsPolicyInputs().addInputsItem(protectedDataPolicy);

  @Test
  void testProtectedDataPolicyDoUndo() throws Exception {
    CreateSnapshotPolicyStep step = new CreateSnapshotPolicyStep(policyService, true, SNAPSHOT_ID);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(policyService).createOrUpdatePao(SNAPSHOT_ID, TpsObjectType.SNAPSHOT, policies);
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(policyService).deletePaoIfExists(SNAPSHOT_ID);
  }

  @Test
  void testNoPolicyDoUndo() throws Exception {
    CreateSnapshotPolicyStep step = new CreateSnapshotPolicyStep(policyService, false, SNAPSHOT_ID);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(policyService, never()).createOrUpdatePao(SNAPSHOT_ID, TpsObjectType.SNAPSHOT, policies);
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(policyService, never()).deletePaoIfExists(SNAPSHOT_ID);
  }
}

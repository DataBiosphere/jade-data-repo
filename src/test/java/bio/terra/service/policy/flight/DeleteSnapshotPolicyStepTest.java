package bio.terra.service.policy.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.*;

import bio.terra.common.exception.FeatureNotImplementedException;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.snapshot.flight.delete.DeleteSnapshotPolicyStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag("bio.terra.common.category.Unit")
public class DeleteSnapshotPolicyStepTest {
  @Mock private PolicyService policyService;
  @Mock private FlightContext flightContext;

  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private DeleteSnapshotPolicyStep step;

  @BeforeEach
  public void setup() throws Exception {
    step = new DeleteSnapshotPolicyStep(policyService, SNAPSHOT_ID);
  }

  @Test
  void testDeletePolicyDoUndo() throws Exception {
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(policyService).deletePao(SNAPSHOT_ID);
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
  }

  @Test
  void testDeletePolicyServiceNotEnabled() throws Exception {
    var exception = new FeatureNotImplementedException("Policy service is not enabled");
    doThrow(exception).when(policyService).deletePao(SNAPSHOT_ID);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(policyService).deletePao(SNAPSHOT_ID);
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
  }
}

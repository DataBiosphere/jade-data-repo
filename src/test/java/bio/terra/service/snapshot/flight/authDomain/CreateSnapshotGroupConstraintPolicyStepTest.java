package bio.terra.service.snapshot.flight.authDomain;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;

import bio.terra.common.category.Unit;
import bio.terra.policy.model.TpsObjectType;
import bio.terra.policy.model.TpsPaoUpdateRequest;
import bio.terra.policy.model.TpsPolicyInput;
import bio.terra.policy.model.TpsPolicyInputs;
import bio.terra.service.policy.PolicyService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.List;
import java.util.UUID;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.context.ActiveProfiles;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class CreateSnapshotGroupConstraintPolicyStepTest {

  @Mock private PolicyService policyService;
  @Mock private FlightContext flightContext;

  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final List<String> userGroups = List.of("group1", "group2");

  @Test
  public void testDoAndUndoStepSucceeds() throws InterruptedException {
    CreateSnapshotGroupConstraintPolicyStep step =
        new CreateSnapshotGroupConstraintPolicyStep(policyService, SNAPSHOT_ID, userGroups);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    List<TpsPolicyInput> inputs =
        userGroups.stream().map(PolicyService::getGroupConstraintPolicyInput).toList();
    TpsPolicyInputs policyInputs = new TpsPolicyInputs().inputs(inputs);
    verify(policyService).createOrUpdatePao(SNAPSHOT_ID, TpsObjectType.SNAPSHOT, policyInputs);

    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    TpsPaoUpdateRequest removeRequest = new TpsPaoUpdateRequest().removeAttributes(policyInputs);
    verify(policyService).updatePao(removeRequest, SNAPSHOT_ID);
  }
}

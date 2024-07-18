package bio.terra.service.snapshot.flight.authDomain;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.policy.model.TpsObjectType;
import bio.terra.policy.model.TpsPaoUpdateRequest;
import bio.terra.policy.model.TpsPolicyInput;
import bio.terra.policy.model.TpsPolicyInputs;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class CreateSnapshotGroupConstraintPolicyStepTest {

  @Mock private PolicyService policyService;

  private static final UUID SNAPSHOT_ID = UUID.randomUUID();

  @Test
  void testDoAndUndoStepSucceeds() throws InterruptedException {
    List<String> userGroups = new ArrayList<>(List.of("group1", "group2"));
    FlightContext flightContext = mock(FlightContext.class);
    var flightMap = new FlightMap();
    flightMap.put(SnapshotWorkingMapKeys.SNAPSHOT_DATA_ACCESS_CONTROL_GROUPS, userGroups);
    when(flightContext.getWorkingMap()).thenReturn(flightMap);
    CreateSnapshotGroupConstraintPolicyStep step =
        new CreateSnapshotGroupConstraintPolicyStep(policyService, SNAPSHOT_ID);
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

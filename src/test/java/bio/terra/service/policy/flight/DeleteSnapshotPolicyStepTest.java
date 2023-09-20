package bio.terra.service.policy.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.*;

import bio.terra.policy.model.TpsPolicyInput;
import bio.terra.policy.model.TpsPolicyInputs;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.policy.PolicyService;
import bio.terra.service.snapshot.flight.delete.DeleteSnapshotPolicyStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
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
  @Mock private DatasetService datasetService;
  @Mock private PolicyService policyService;
  @Mock private FlightContext flightContext;

  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final TpsPolicyInput protectedDataPolicy =
      PolicyService.getProtectedDataPolicyInput();
  private static final TpsPolicyInputs policies =
      new TpsPolicyInputs().addInputsItem(protectedDataPolicy);
  private DeleteSnapshotPolicyStep step;

  @BeforeEach
  public void setup() throws Exception {
    FlightMap workingMap = new FlightMap();
    workingMap.put(DatasetWorkingMapKeys.DATASET_ID, DATASET_ID);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    step = new DeleteSnapshotPolicyStep(datasetService, policyService, SNAPSHOT_ID);
  }

  private void mockSecureMonitoringEnabledDataset() {
    when(datasetService.retrieve(DATASET_ID))
        .thenReturn(new Dataset(new DatasetSummary().secureMonitoringEnabled(true)));
  }

  @Test
  void testDeletePolicyDoUndo() throws Exception {
    mockSecureMonitoringEnabledDataset();
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(policyService).deletePaoIfExists(SNAPSHOT_ID);

    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(policyService).createSnapshotPao(SNAPSHOT_ID, policies);
  }

  @Test
  void testNoPolicyToDelete() throws Exception {
    when(datasetService.retrieve(DATASET_ID)).thenReturn(new Dataset());
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verifyNoInteractions(policyService);
  }
}

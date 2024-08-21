package bio.terra.service.load.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadService;
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
@Tag(Unit.TAG)
class LoadUnlockStepTest {
  @Mock private LoadService loadService;
  @Mock private FlightContext flightContext;
  private LoadUnlockStep step;

  private static final String FLIGHT_ID = "flightId";
  private static final String LOAD_TAG = "loadTag";
  private static final UUID DATASET_ID = UUID.randomUUID();

  @BeforeEach
  void setup() {
    when(flightContext.getFlightId()).thenReturn(FLIGHT_ID);
    when(loadService.getLoadTag(flightContext)).thenReturn(LOAD_TAG);

    FlightMap inputParams = new FlightMap();
    inputParams.put(JobMapKeys.DATASET_ID.getKeyName(), DATASET_ID);
    when(flightContext.getInputParameters()).thenReturn(inputParams);

    step = new LoadUnlockStep(loadService);
  }

  @Test
  void doStep() {
    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    assertThat(doResult.getException().isPresent(), equalTo(false));

    verify(loadService).unlockLoad(LOAD_TAG, FLIGHT_ID, DATASET_ID);
  }
}

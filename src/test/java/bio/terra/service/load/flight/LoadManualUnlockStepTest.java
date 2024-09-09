package bio.terra.service.load.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadLockKey;
import bio.terra.service.load.LoadService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class LoadManualUnlockStepTest {
  @Mock private LoadService loadService;
  @Mock private FlightContext flightContext;
  private LoadManualUnlockStep step;
  private FlightMap inputParameters;
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final String LOCK_NAME = "flightToManuallyUnlock";

  @BeforeEach
  void setup() {
    inputParameters = new FlightMap();
    when(flightContext.getInputParameters()).thenReturn(inputParameters);

    step = new LoadManualUnlockStep(loadService, LOCK_NAME);
  }

  @Test
  void doStep() {
    inputParameters.put(JobMapKeys.DATASET_ID.getKeyName(), DATASET_ID);
    assertThat(step.doStep(flightContext), equalTo(StepResult.getStepResultSuccess()));

    verify(loadService).unlockLoad(new LoadLockKey(DATASET_ID), LOCK_NAME);
  }

  @Test
  void doStep_missingDatasetId() {
    // Without the dataset ID as an input parameter, this step throws.
    assertThrows(IllegalStateException.class, () -> step.doStep(flightContext));

    verify(loadService, never()).unlockLoad(any(), any());
  }
}

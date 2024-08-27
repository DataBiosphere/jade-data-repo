package bio.terra.service.load.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.load.LoadLockKey;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.exception.LoadLockFailureException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.UUID;
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
  private static final String LOCK_NAME = "suppliedLockName";
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final LoadLockKey LOAD_LOCK_KEY = new LoadLockKey("loadTag", DATASET_ID);

  private void mockFlightId() {
    when(flightContext.getFlightId()).thenReturn(FLIGHT_ID);
  }

  private void mockDatasetId() {
    FlightMap inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.DATASET_ID.getKeyName(), DATASET_ID);
    when(flightContext.getInputParameters()).thenReturn(inputParameters);
  }

  @Test
  void doStep_flightId() {
    mockFlightId();
    when(loadService.getLoadLockKey(flightContext)).thenReturn(LOAD_LOCK_KEY);
    step = new LoadUnlockStep(loadService);

    assertThat(step.doStep(flightContext), equalTo(StepResult.getStepResultSuccess()));

    // When a lockName isn't supplied to step construction, the active flight ID is passed to our
    // unlock load call.
    verify(loadService).unlockLoad(LOAD_LOCK_KEY, FLIGHT_ID);
  }

  @Test
  void doStep_lockName() {
    when(loadService.getLoadLockKey(flightContext)).thenReturn(LOAD_LOCK_KEY);
    step = new LoadUnlockStep(loadService, LOCK_NAME);

    assertThat(step.doStep(flightContext), equalTo(StepResult.getStepResultSuccess()));

    // When a lockName is supplied to step construction, it's passed to our unlock load call.
    verify(loadService).unlockLoad(LOAD_LOCK_KEY, LOCK_NAME);
  }

  @Test
  void doStep_unlockLoadByDataset() {
    mockDatasetId();
    doThrow(LoadLockFailureException.class).when(loadService).getLoadLockKey(flightContext);
    step = new LoadUnlockStep(loadService, LOCK_NAME);

    assertThat(step.doStep(flightContext), equalTo(StepResult.getStepResultSuccess()));

    // When a loadTag to unlock isn't available in the flight context, we unlock by dataset ID only.
    verify(loadService).unlockLoad(DATASET_ID, LOCK_NAME);
  }
}

package bio.terra.service.load.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.exception.ConflictException;
import bio.terra.service.load.LoadLockKey;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.exception.LoadLockedException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.Optional;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class LoadLockStepTest {
  @Mock private LoadService loadService;
  @Mock private FlightContext flightContext;
  private LoadLockStep step;
  private FlightMap workingMap;

  private static final String FLIGHT_ID = "flightId";
  private static final LoadLockKey LOAD_LOCK_KEY = new LoadLockKey("loadTag", UUID.randomUUID());
  private static final UUID LOAD_ID = UUID.randomUUID();

  @BeforeEach
  void setup() {
    workingMap = new FlightMap();
    when(flightContext.getFlightId()).thenReturn(FLIGHT_ID);
    when(loadService.getLoadLockKey(flightContext)).thenReturn(LOAD_LOCK_KEY);

    step = new LoadLockStep(loadService);
  }

  @Test
  void doStep_success() throws InterruptedException {
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(loadService.lockLoad(LOAD_LOCK_KEY, FLIGHT_ID)).thenReturn(LOAD_ID);

    assertThat(step.doStep(flightContext), equalTo(StepResult.getStepResultSuccess()));

    assertThat(workingMap.get(LoadMapKeys.LOAD_ID, UUID.class), equalTo(LOAD_ID));
  }

  @Test
  void doStep_retryLoadLockedException() throws InterruptedException {
    var exception = new LoadLockedException("Load tag is locked by another flight");
    doThrow(exception).when(loadService).lockLoad(LOAD_LOCK_KEY, FLIGHT_ID);

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_RETRY));
    Optional<Exception> actualMaybeException = doResult.getException();
    assertThat(actualMaybeException.isPresent(), equalTo(true));
    assertThat(actualMaybeException.get(), equalTo(exception));
    assertFalse(workingMap.containsKey(LoadMapKeys.LOAD_ID));
  }

  @Test
  void doStep_throwsUnhandledException() {
    doThrow(ConflictException.class).when(loadService).lockLoad(LOAD_LOCK_KEY, FLIGHT_ID);

    assertThrows(ConflictException.class, () -> step.doStep(flightContext));

    assertFalse(workingMap.containsKey(LoadMapKeys.LOAD_ID));
  }

  @Test
  void undoStep() {
    assertThat(step.undoStep(flightContext), equalTo(StepResult.getStepResultSuccess()));

    verify(loadService).unlockLoad(LOAD_LOCK_KEY, FLIGHT_ID);
  }
}

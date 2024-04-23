package bio.terra.service.load.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.exception.ConflictException;
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
  private static final String LOAD_TAG = "loadTag";
  private static final UUID LOAD_ID = UUID.randomUUID();

  @BeforeEach
  void setup() {
    workingMap = new FlightMap();
    when(flightContext.getFlightId()).thenReturn(FLIGHT_ID);
    when(loadService.getLoadTag(flightContext)).thenReturn(LOAD_TAG);

    step = new LoadLockStep(loadService);
  }

  @Test
  void doStep_success() throws InterruptedException {
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(loadService.lockLoad(LOAD_TAG, FLIGHT_ID)).thenReturn(LOAD_ID);

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    assertThat(doResult.getException().isPresent(), equalTo(false));

    assertThat(workingMap.get(LoadMapKeys.LOAD_ID, UUID.class), equalTo(LOAD_ID));
  }

  @Test
  void doStep_retryLoadLockedException() throws InterruptedException {
    var exception = new LoadLockedException("Load tag is locked by another flight");
    doThrow(exception).when(loadService).lockLoad(LOAD_TAG, FLIGHT_ID);

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_RETRY));
    Optional<Exception> actualMaybeException = doResult.getException();
    assertThat(actualMaybeException.isPresent(), equalTo(true));
    assertThat(actualMaybeException.get(), equalTo(exception));
    assertThat(workingMap.get(LoadMapKeys.LOAD_ID, UUID.class), equalTo(null));
  }

  @Test
  void doStep_throwsUnhandledException() throws InterruptedException {
    doThrow(ConflictException.class).when(loadService).lockLoad(LOAD_TAG, FLIGHT_ID);

    assertThrows(ConflictException.class, () -> step.doStep(flightContext));

    assertThat(workingMap.get(LoadMapKeys.LOAD_ID, UUID.class), equalTo(null));
  }
}

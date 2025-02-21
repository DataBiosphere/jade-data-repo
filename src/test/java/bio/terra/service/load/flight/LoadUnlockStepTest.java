package bio.terra.service.load.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.load.LoadLockKey;
import bio.terra.service.load.LoadService;
import bio.terra.stairway.FlightContext;
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
class LoadUnlockStepTest {
  @Mock private LoadService loadService;
  @Mock private FlightContext flightContext;
  private LoadUnlockStep step;

  private static final String FLIGHT_ID = "flightId";
  private static final LoadLockKey LOAD_LOCK_KEY = new LoadLockKey("loadTag", UUID.randomUUID());

  @BeforeEach
  void setup() {
    when(flightContext.getFlightId()).thenReturn(FLIGHT_ID);
    when(loadService.getLoadLockKey(flightContext)).thenReturn(LOAD_LOCK_KEY);

    step = new LoadUnlockStep(loadService);
  }

  @Test
  void doStep() {
    assertThat(step.doStep(flightContext), equalTo(StepResult.getStepResultSuccess()));

    verify(loadService).unlockLoad(LOAD_LOCK_KEY, FLIGHT_ID);
  }
}

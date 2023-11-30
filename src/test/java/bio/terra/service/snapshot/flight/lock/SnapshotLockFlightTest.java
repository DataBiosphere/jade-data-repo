package bio.terra.service.snapshot.flight.lock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.snapshot.flight.LockSnapshotStep;
import bio.terra.stairway.FlightMap;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SnapshotLockFlightTest {
  @Mock private ApplicationContext context;
  private FlightMap inputParameters;
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();

  @BeforeEach
  void setUp() {
    FlightTestUtils.mockFlightSetup(context);

    inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.SNAPSHOT_ID.getKeyName(), SNAPSHOT_ID.toString());
  }

  @Test
  void testCorrectStepsSnapshotLockFlight() {
    var flight = new SnapshotLockFlight(inputParameters, context);

    var steps =
        flight.getSteps().stream()
            .map(step -> step.getClass().getSimpleName())
            .collect(Collectors.toList());
    assertThat(
        steps,
        CoreMatchers.is(
            List.of(
                "LockSnapshotStep",
                "JournalRecordUpdateEntryStep",
                "SnapshotLockSetResponseStep")));
  }

  @Test
  void testParametersForLockStep() {
    var flight = new SnapshotLockFlight(inputParameters, context);
    var firstStep = flight.getSteps().get(0);
    LockSnapshotStep lockSnapshotStep = (LockSnapshotStep) firstStep;
    assertThat(
        "Dataset lock step should not suppresses exceptions",
        lockSnapshotStep.shouldSuppressNotFoundException(),
        is(false));
  }
}

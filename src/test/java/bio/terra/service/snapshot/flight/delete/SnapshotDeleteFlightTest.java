package bio.terra.service.snapshot.flight.delete;

import static bio.terra.common.FlightTestUtils.mockFlightAppConfigSetup;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.snapshot.flight.LockSnapshotStep;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SnapshotDeleteFlightTest {
  @Mock private ApplicationContext context;
  private FlightMap inputParameters;
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();

  @BeforeEach
  void beforeEach() {
    mockFlightAppConfigSetup(context);

    inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.SNAPSHOT_ID.getKeyName(), SNAPSHOT_ID.toString());
  }

  @Test
  void testSnapshotDeleteLocksSnapshot() {
    var flight = new SnapshotDeleteFlight(inputParameters, context);

    Step firstStep = flight.getSteps().get(0);
    assertThat(
        "Snapshot deletion flight locks the snapshot first",
        firstStep,
        instanceOf(LockSnapshotStep.class));
    assertThat(
        "Snapshot lock step suppresses 'snapshot not found' exceptions",
        ((LockSnapshotStep) firstStep).shouldSuppressNotFoundException(),
        is(true));
  }
}

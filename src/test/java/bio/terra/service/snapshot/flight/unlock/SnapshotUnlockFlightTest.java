package bio.terra.service.snapshot.flight.unlock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.UnlockResourceRequest;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.snapshot.flight.UnlockSnapshotStep;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.context.ApplicationContext;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SnapshotUnlockFlightTest {
  @Mock private ApplicationContext context;
  private FlightMap inputParameters;
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final String LOCK_NAME = "lock-name";

  @BeforeEach
  void setUp() {
    FlightTestUtils.mockFlightAppConfigSetup(context);

    inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.SNAPSHOT_ID.getKeyName(), SNAPSHOT_ID.toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCorrectStepsSnapshotUnlockFlight(boolean forceUnlock) {
    inputParameters.put(
        JobMapKeys.REQUEST.getKeyName(),
        new UnlockResourceRequest().lockName(LOCK_NAME).forceUnlock(forceUnlock));
    var flight = new SnapshotUnlockFlight(inputParameters, context);

    var steps = FlightTestUtils.getStepNames(flight);
    if (forceUnlock) {
      assertThat(
          steps,
          contains(
              "UnlockSnapshotCheckLockNameStep",
              "UnlockResourceCheckJobStateStep",
              "UnlockSnapshotStep",
              "JournalRecordUpdateEntryStep"));
    } else {
      assertThat(
          steps,
          contains(
              "UnlockSnapshotCheckLockNameStep",
              "UnlockSnapshotStep",
              "JournalRecordUpdateEntryStep"));
    }
  }

  @Test
  void testParametersForUnlockStep() {
    inputParameters.put(
        JobMapKeys.REQUEST.getKeyName(),
        new UnlockResourceRequest().lockName(LOCK_NAME).forceUnlock(false));
    var flight = new SnapshotUnlockFlight(inputParameters, context);
    var secondStep = flight.getSteps().get(1);
    UnlockSnapshotStep unlockSnapshotStep = (UnlockSnapshotStep) secondStep;
    assertThat(
        "The lock name is passed to the unlock method",
        unlockSnapshotStep.getLockName(),
        equalTo(LOCK_NAME));
  }
}

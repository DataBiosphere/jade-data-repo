package bio.terra.service.dataset.flight.unlock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.UnlockResourceRequest;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.job.JobMapKeys;
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
class DatasetUnlockFlightTest {
  @Mock private ApplicationContext context;
  private FlightMap inputParameters;
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final String LOCK_NAME = "lock-name";

  @BeforeEach
  void setUp() {
    FlightTestUtils.mockFlightAppConfigSetup(context);

    inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.DATASET_ID.getKeyName(), DATASET_ID.toString());
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testCorrectStepsDatasetUnlockFlight(boolean forceUnlock) {
    inputParameters.put(
        JobMapKeys.REQUEST.getKeyName(),
        new UnlockResourceRequest().lockName(LOCK_NAME).forceUnlock(forceUnlock));
    var flight = new DatasetUnlockFlight(inputParameters, context);

    var steps = FlightTestUtils.getStepNames(flight);
    if (forceUnlock) {
      assertThat(
          steps,
          contains(
              "UnlockDatasetCheckLockNameStep",
              "UnlockResourceCheckJobStateStep",
              "UnlockDatasetStep",
              "JournalRecordUpdateEntryStep",
              "DatasetLockSetResponseStep"));
    } else {
      assertThat(
          steps,
          contains(
              "UnlockDatasetCheckLockNameStep",
              "UnlockDatasetStep",
              "JournalRecordUpdateEntryStep",
              "DatasetLockSetResponseStep"));
    }
  }

  @Test
  void testParametersForUnlockStep() {
    inputParameters.put(
        JobMapKeys.REQUEST.getKeyName(),
        new UnlockResourceRequest().lockName(LOCK_NAME).forceUnlock(false));
    var flight = new DatasetUnlockFlight(inputParameters, context);
    var secondStep = flight.getSteps().get(1);
    UnlockDatasetStep unlockDatasetStep = (UnlockDatasetStep) secondStep;
    assertThat("Unlock the Exclusive lock on dataset", unlockDatasetStep.isSharedLock(), is(false));
    assertThat("Throw lock exception", unlockDatasetStep.isThrowLockException());
    assertThat(
        "The lock name is passed to the unlock method",
        unlockDatasetStep.getLockName(),
        equalTo(LOCK_NAME));
  }
}

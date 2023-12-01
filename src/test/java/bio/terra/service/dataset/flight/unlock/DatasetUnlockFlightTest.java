package bio.terra.service.dataset.flight.unlock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.service.dataset.flight.UnlockDatasetStep;
import bio.terra.service.job.JobMapKeys;
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
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), LOCK_NAME);
  }

  @Test
  void testCorrectStepsDatasetUnlockFlight() {
    var flight = new DatasetUnlockFlight(inputParameters, context);

    var steps =
        flight.getSteps().stream()
            .map(step -> step.getClass().getSimpleName())
            .collect(Collectors.toList());
    assertThat(
        steps, CoreMatchers.is(List.of("UnlockDatasetStep", "JournalRecordUpdateEntryStep")));
  }

  @Test
  void testParametersForUnlockStep() {
    var flight = new DatasetUnlockFlight(inputParameters, context);
    var firstStep = flight.getSteps().get(0);
    UnlockDatasetStep unlockDatasetStep = (UnlockDatasetStep) firstStep;
    assertThat("Unlock the Exclusive lock on dataset", unlockDatasetStep.isSharedLock(), is(false));
    assertThat(
        "The lock name is passed to the unlock method",
        unlockDatasetStep.getLockName(),
        equalTo(LOCK_NAME));
  }
}

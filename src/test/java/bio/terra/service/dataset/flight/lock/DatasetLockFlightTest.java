package bio.terra.service.dataset.flight.lock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.service.dataset.flight.LockDatasetStep;
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
class DatasetLockFlightTest {
  @Mock private ApplicationContext context;
  private FlightMap inputParameters;
  private static final UUID DATASET_ID = UUID.randomUUID();

  @BeforeEach
  void setUp() {
    FlightTestUtils.mockFlightAppConfigSetup(context);

    inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.DATASET_ID.getKeyName(), DATASET_ID.toString());
  }

  @Test
  void testCorrectStepsDatasetLockFlight() {
    var flight = new DatasetLockFlight(inputParameters, context);

    var steps =
        flight.getSteps().stream()
            .map(step -> step.getClass().getSimpleName())
            .collect(Collectors.toList());
    assertThat(
        steps,
        CoreMatchers.is(
            List.of(
                "LockDatasetStep", "JournalRecordUpdateEntryStep", "DatasetLockSetResponseStep")));
  }

  @Test
  void testParametersForLockStep() {
    var flight = new DatasetLockFlight(inputParameters, context);
    var firstStep = flight.getSteps().get(0);
    LockDatasetStep lockDatasetStep = (LockDatasetStep) firstStep;
    assertThat("Exclusive lock on dataset is obtained", lockDatasetStep.isSharedLock(), is(false));
    assertThat(
        "Dataset lock step should not suppresses exceptions",
        lockDatasetStep.shouldSuppressNotFoundException(),
        is(false));
  }
}

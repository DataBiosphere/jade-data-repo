package bio.terra.service.dataset.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import bio.terra.common.FlightTestUtils;
import bio.terra.common.category.Unit;
import bio.terra.service.dataset.flight.delete.DatasetDeleteFlight;
import bio.terra.service.job.JobMapKeys;
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
public class DatasetDeleteFlightTest {
  @Mock private ApplicationContext context;
  private FlightMap inputParameters;
  private static final UUID DATASET_ID = UUID.randomUUID();

  @BeforeEach
  void beforeEach() {
    FlightTestUtils.mockFlightAppConfigSetup(context);

    inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.DATASET_ID.getKeyName(), DATASET_ID.toString());
  }

  @Test
  void testSnapshotDeleteLocksSnapshot() {
    var flight = new DatasetDeleteFlight(inputParameters, context);

    Step firstStep = flight.getSteps().get(0);
    assertThat(
        "Dataset deletion flight locks the dataset first",
        firstStep,
        instanceOf(LockDatasetStep.class));
    LockDatasetStep lockDatasetStep = (LockDatasetStep) firstStep;
    assertThat("Exclusive lock on dataset is obtained", lockDatasetStep.isSharedLock(), is(false));
    assertThat(
        "Dataset lock step suppresses 'snapshot not found' exceptions",
        lockDatasetStep.shouldSuppressNotFoundException(),
        is(true));
  }
}

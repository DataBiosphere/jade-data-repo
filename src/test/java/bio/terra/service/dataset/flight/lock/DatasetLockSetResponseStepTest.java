package bio.terra.service.dataset.flight.lock;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import bio.terra.common.StepUtils;
import bio.terra.common.category.Unit;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.model.ResourceLocks;
import bio.terra.service.dataset.DatasetService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class DatasetLockSetResponseStepTest {
  private static final UUID DATASET_ID = UUID.randomUUID();
  private DatasetLockSetResponseStep step;
  @Mock private FlightContext flightContext;
  @Mock private DatasetService datasetService;

  @Test
  void doStep() {
    // Setup
    step = new DatasetLockSetResponseStep(datasetService, DATASET_ID);
    when(flightContext.getWorkingMap()).thenReturn(new FlightMap());
    var lockName = "lock123";
    var locks = new ResourceLocks().exclusive(lockName);
    var datasetSummaryModel = new DatasetSummaryModel().resourceLocks(locks);
    when(datasetService.retrieveDatasetSummary(DATASET_ID)).thenReturn(datasetSummaryModel);
    StepUtils.readInputs(step, flightContext);

    // Perform Step
    step.perform();

    // Confirm Response is correctly set
    FlightMap workingMap = flightContext.getWorkingMap();
    assertThat("Response is the ResourceLocks object", step.getResponse(), equalTo(locks));
    assertThat("Response Status is HttpStatus.OK", step.getStatusCode(), equalTo(HttpStatus.OK));
  }
}

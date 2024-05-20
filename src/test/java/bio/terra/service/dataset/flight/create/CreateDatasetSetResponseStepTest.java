package bio.terra.service.dataset.flight.create;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.DatasetSummaryModel;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class CreateDatasetSetResponseStepTest {

  @Mock private DatasetService datasetService;
  @Mock private FlightContext flightContext;
  private FlightMap workingMap;
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final DatasetSummaryModel DATASET_SUMMARY =
      new DatasetSummaryModel().id(DATASET_ID).name("Dataset summary for response");
  private CreateDatasetSetResponseStep step;

  @BeforeEach
  void setup() {
    workingMap = new FlightMap();
    workingMap.put(DatasetWorkingMapKeys.DATASET_ID, DATASET_ID);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(flightContext.getInputParameters()).thenReturn(new FlightMap());

    when(datasetService.retrieveDatasetSummary(DATASET_ID)).thenReturn(DATASET_SUMMARY);
  }

  @Test
  void testDoStep() throws InterruptedException {
    step = new CreateDatasetSetResponseStep(datasetService);

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    assertThat(
        "Dataset summary is written to working map as response",
        workingMap.get(JobMapKeys.RESPONSE.getKeyName(), DatasetSummaryModel.class),
        equalTo(DATASET_SUMMARY));
    assertThat(
        "Created is written to working map as job status",
        workingMap.get(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.class),
        equalTo(HttpStatus.CREATED));
  }
}

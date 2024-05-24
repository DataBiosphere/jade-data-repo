package bio.terra.service.snapshot.flight.create;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
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
class CreateSnapshotSetResponseStepTest {

  @Mock private SnapshotService snapshotService;
  @Mock private FlightContext flightContext;
  private FlightMap workingMap;
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final SnapshotSummaryModel SNAPSHOT_SUMMARY =
      new SnapshotSummaryModel().id(SNAPSHOT_ID).name("Snapshot summary for response");
  private CreateSnapshotSetResponseStep step;

  @BeforeEach
  void setup() {
    workingMap = new FlightMap();
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_ID, SNAPSHOT_ID);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);

    when(snapshotService.retrieveSnapshotSummary(SNAPSHOT_ID)).thenReturn(SNAPSHOT_SUMMARY);
  }

  @Test
  void testDoStep() {
    step = new CreateSnapshotSetResponseStep(snapshotService, SNAPSHOT_ID);

    StepResult doResult = step.doStep(flightContext);

    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    assertThat(
        "Snapshot summary is written to working map as response",
        workingMap.get(JobMapKeys.RESPONSE.getKeyName(), SnapshotSummaryModel.class),
        equalTo(SNAPSHOT_SUMMARY));
    assertThat(
        "Created is written to working map as job status",
        workingMap.get(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.class),
        equalTo(HttpStatus.CREATED));
  }
}

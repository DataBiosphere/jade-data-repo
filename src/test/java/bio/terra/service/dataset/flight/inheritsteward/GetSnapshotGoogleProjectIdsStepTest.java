package bio.terra.service.dataset.flight.inheritsteward;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class GetSnapshotGoogleProjectIdsStepTest {

  @Mock private SnapshotService snapshotService;
  @Mock private FlightContext flightContext;

  private GetSnapshotGoogleProjectIdsStep step;
  private final UUID datasetId = UUID.randomUUID();

  @BeforeEach
  void beforeEach() {
    step = new GetSnapshotGoogleProjectIdsStep(snapshotService, datasetId);
  }

  @Test
  void doStep() throws Exception {
    var projectIds = Arrays.asList("project1", "project2");
    FlightMap workingMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(snapshotService.getSnapshotGoogleProjectIds(datasetId)).thenReturn(projectIds);
    assertThat(step.doStep(flightContext), is(StepResult.getStepResultSuccess()));
    assertThat(
        workingMap.get(DatasetWorkingMapKeys.SNAPSHOT_GOOGLE_PROJECT_IDS, List.class),
        is(projectIds));
  }

  @Test
  void undoStep() throws Exception {
    assertThat(step.undoStep(flightContext), is(StepResult.getStepResultSuccess()));
    verifyNoInteractions(snapshotService, flightContext);
  }
}

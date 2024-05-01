package bio.terra.service.snapshot.flight.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.DuosFixtures;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.service.duos.DuosDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class RetrieveDuosFirecloudGroupStepTest {

  @Mock private DuosDao duosDao;
  @Mock private FlightContext flightContext;

  private static final String DUOS_ID = "DUOS-123456";
  private static final DuosFirecloudGroupModel RETRIEVED =
      DuosFixtures.createDbFirecloudGroup(DUOS_ID);

  private RetrieveDuosFirecloudGroupStep step;
  private FlightMap workingMap;

  @BeforeEach
  void setup() {
    step = new RetrieveDuosFirecloudGroupStep(duosDao, DUOS_ID);

    workingMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  void testDoStepGroupExists() throws InterruptedException {
    when(duosDao.retrieveFirecloudGroupByDuosId(DUOS_ID)).thenReturn(RETRIEVED);

    StepResult result = step.doStep(flightContext);
    assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(duosDao).retrieveFirecloudGroupByDuosId(DUOS_ID);

    assertThat(SnapshotDuosFlightUtils.getFirecloudGroup(flightContext), equalTo(RETRIEVED));
    assertTrue(workingMap.get(SnapshotDuosMapKeys.FIRECLOUD_GROUP_RETRIEVED, boolean.class));
  }

  @Test
  void testDoStepGroupDoesNotExist() throws InterruptedException {
    StepResult result = step.doStep(flightContext);
    assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(duosDao).retrieveFirecloudGroupByDuosId(DUOS_ID);

    assertNull(SnapshotDuosFlightUtils.getFirecloudGroup(flightContext));
    assertFalse(workingMap.get(SnapshotDuosMapKeys.FIRECLOUD_GROUP_RETRIEVED, boolean.class));
  }
}

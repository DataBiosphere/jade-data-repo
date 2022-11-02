package bio.terra.service.snapshot.flight.duos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.times;
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
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class RetrieveDuosFirecloudGroupStepTest {

  @MockBean private DuosDao duosDao;
  @Mock private FlightContext flightContext;

  private static final String DUOS_ID = "DUOS-123456";

  private RetrieveDuosFirecloudGroupStep step;
  private DuosFirecloudGroupModel duosFirecloudGroupRetrieved;
  private FlightMap workingMap;

  @Before
  public void setup() {
    step = new RetrieveDuosFirecloudGroupStep(duosDao, DUOS_ID);

    duosFirecloudGroupRetrieved = DuosFixtures.duosFirecloudGroupFromDb(DUOS_ID);

    workingMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  public void testDoStepGroupExists() throws InterruptedException {
    when(duosDao.retrieveFirecloudGroupByDuosId(DUOS_ID)).thenReturn(duosFirecloudGroupRetrieved);

    StepResult result = step.doStep(flightContext);
    assertEquals(result.getStepStatus(), StepStatus.STEP_RESULT_SUCCESS);
    verify(duosDao, times(1)).retrieveFirecloudGroupByDuosId(DUOS_ID);

    assertEquals(
        SnapshotDuosFlightUtils.getFirecloudGroup(flightContext), duosFirecloudGroupRetrieved);
    assertTrue(workingMap.get(SnapshotDuosMapKeys.FIRECLOUD_GROUP_RETRIEVED, boolean.class));
  }

  @Test
  public void testDoStepGroupDoesNotExist() throws InterruptedException {
    StepResult result = step.doStep(flightContext);
    assertEquals(result.getStepStatus(), StepStatus.STEP_RESULT_SUCCESS);
    verify(duosDao, times(1)).retrieveFirecloudGroupByDuosId(DUOS_ID);

    assertNull(SnapshotDuosFlightUtils.getFirecloudGroup(flightContext));
    assertFalse(workingMap.get(SnapshotDuosMapKeys.FIRECLOUD_GROUP_RETRIEVED, boolean.class));
  }
}

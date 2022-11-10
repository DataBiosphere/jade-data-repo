package bio.terra.service.snapshot.flight.duos;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.context.ActiveProfiles;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class IfNoGroupRetrievedStepTest {

  @Mock private Step innerStep;
  @Mock private FlightContext flightContext;

  private IfNoGroupRetrievedStep step;
  private FlightMap workingMap;

  @Before
  public void setup() {
    step = new IfNoGroupRetrievedStep(innerStep);

    workingMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  public void testEnabled() {
    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP_RETRIEVED, true);
    assertFalse(
        "We should not create a Firecloud group when one has been retrieved",
        step.isEnabled(flightContext));

    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP_RETRIEVED, false);
    assertTrue(
        "We should create a Firecloud group when one hasn't been retrieved",
        step.isEnabled(flightContext));
  }
}

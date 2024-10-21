package bio.terra.service.snapshot.flight.duos;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class IfNoGroupRetrievedStepTest {

  @Mock private FlightContext flightContext;

  private IfNoGroupRetrievedStep step;
  private FlightMap workingMap;

  @BeforeEach
  void setup() {
    step = new IfNoGroupRetrievedStep(mock(Step.class));

    workingMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  void testEnabled() {
    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP_RETRIEVED, true);
    assertFalse(
        step.isEnabled(flightContext),
        "We should not create a Firecloud group when one has been retrieved");

    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP_RETRIEVED, false);
    assertTrue(
        step.isEnabled(flightContext),
        "We should create a Firecloud group when one hasn't been retrieved");
  }
}

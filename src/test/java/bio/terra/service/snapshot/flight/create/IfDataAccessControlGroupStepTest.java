package bio.terra.service.snapshot.flight.create;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class IfDataAccessControlGroupStepTest {
  @Mock private FlightContext flightContext;
  @Mock private Step step;

  @Test
  void isEnabled() throws InterruptedException {
    var flightMap = new FlightMap();
    List<String> userGroups = new ArrayList<>(List.of("group1", "group2"));
    flightMap.put(SnapshotWorkingMapKeys.SNAPSHOT_DATA_ACCESS_CONTROL_GROUPS, userGroups);
    when(flightContext.getWorkingMap()).thenReturn(flightMap);
    // Strict mocking shows that we run the nested "doStep" when the optional step is enabled
    when(step.doStep(flightContext)).thenReturn(StepResult.getStepResultSuccess());
    IfDataAccessControlGroupStep step = new IfDataAccessControlGroupStep(this.step);
    step.doStep(flightContext);
    assertTrue(step.isEnabled(flightContext));
  }

  @Test
  void isDisabled() throws InterruptedException {
    var flightMap = new FlightMap();
    // No user groups, so step should be disabled
    List<String> userGroups = new ArrayList<>();
    flightMap.put(SnapshotWorkingMapKeys.SNAPSHOT_DATA_ACCESS_CONTROL_GROUPS, userGroups);
    when(flightContext.getWorkingMap()).thenReturn(flightMap);
    IfDataAccessControlGroupStep step = new IfDataAccessControlGroupStep(this.step);
    step.doStep(flightContext);
    assertFalse(step.isEnabled(flightContext));
  }
}

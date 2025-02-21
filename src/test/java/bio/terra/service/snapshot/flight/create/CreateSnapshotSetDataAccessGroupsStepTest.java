package bio.terra.service.snapshot.flight.create;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepStatus;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class CreateSnapshotSetDataAccessGroupsStepTest {

  @Mock private FlightContext flightContext;

  @Test
  void testRequestAndSnapshotGroup() throws InterruptedException {
    // Group set on the request
    List<String> userGroups = new ArrayList<>(List.of("group1", "group2"));
    // Group created for the snapshot byRequestId case
    var inputSnapshotGroup = "group3";

    runAndAssertContains(userGroups, inputSnapshotGroup, "group1", "group2", "group3");
  }

  @Test
  void testUniqueGroups() throws InterruptedException {
    // Group set on the request - duplicate groups
    List<String> userGroups = new ArrayList<>(List.of("group1", "group3", "group3"));
    // Group created for the snapshot byRequestId case - containing duplicate to the request
    var inputSnapshotGroup = "group3";

    runAndAssertContains(userGroups, inputSnapshotGroup, "group1", "group3");
  }

  @Test
  void groupCreatedBySnapshot() throws InterruptedException {
    // No group set on the request
    List<String> userGroups = null;
    // Group created for the snapshot byRequestId case
    var inputSnapshotGroup = "group1";

    runAndAssertContains(userGroups, inputSnapshotGroup, "group1");
  }

  @Test
  void groupsSetOnRequest() throws InterruptedException {
    // Groups set on the request
    List<String> userGroups = new ArrayList<>(List.of("group1", "group2"));
    // No group created for the snapshot byRequestId case; flight map is empty
    String inputSnapshotGroup = null;

    runAndAssertContains(userGroups, inputSnapshotGroup, "group1", "group2");
  }

  private void runAndAssertContains(
      List<String> inputRequestGroups, String inputSnapshotGroup, String... expectedGroups)
      throws InterruptedException {
    FlightMap flightMap = new FlightMap();
    if (inputSnapshotGroup != null) {
      flightMap.put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, inputSnapshotGroup);
    }
    when(flightContext.getWorkingMap()).thenReturn(flightMap);

    CreateSnapshotSetDataAccessGroupsStep step =
        new CreateSnapshotSetDataAccessGroupsStep(inputRequestGroups);
    var result = step.doStep(flightContext);
    assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    List<String> groups =
        flightMap.get(
            SnapshotWorkingMapKeys.SNAPSHOT_DATA_ACCESS_CONTROL_GROUPS, new TypeReference<>() {});
    assertThat(groups, containsInAnyOrder(expectedGroups));
  }
}

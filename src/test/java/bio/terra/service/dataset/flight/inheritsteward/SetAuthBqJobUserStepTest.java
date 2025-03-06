package bio.terra.service.dataset.flight.inheritsteward;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SetAuthBqJobUserStepTest {

  @Mock private ResourceService resourceService;
  @Mock private FlightContext flightContext;

  private void verifySetAuth(boolean inheritSteward) throws Exception {
    String custodianEmail = "custodianEmail";
    SetAuthBqJobUserStep step =
        new SetAuthBqJobUserStep(resourceService, custodianEmail, inheritSteward);
    var projectIds = Arrays.asList("project1", "project2");
    FlightMap workingMap = new FlightMap();
    workingMap.put(DatasetWorkingMapKeys.SNAPSHOT_GOOGLE_PROJECT_IDS, projectIds);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    assertThat(step.doStep(flightContext), is(StepResult.getStepResultSuccess()));
    for (var projectId : projectIds) {
      if (inheritSteward) {
        verify(resourceService).grantPoliciesBqJobUser(projectId, List.of(custodianEmail));
      } else {
        verify(resourceService).revokePoliciesBqJobUser(projectId, List.of(custodianEmail));
      }
    }
  }

  @Test
  void doStep() throws Exception {
    verifySetAuth(true);
  }

  @Test
  void undoStep() throws Exception {
    verifySetAuth(false);
  }
}

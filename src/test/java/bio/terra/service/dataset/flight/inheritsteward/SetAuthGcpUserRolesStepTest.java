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
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SetAuthGcpUserRolesStepTest {

  @Mock private ResourceService resourceService;
  @Mock private FlightContext flightContext;

  private final String custodianEmail = "custodianEmail";

  interface DoOrUndo {
    StepResult apply(FlightContext t) throws Exception;
  }

  private void verifySetAuth(DoOrUndo doOrUndo, boolean grantPolicy) throws Exception {
    var projectIds = Arrays.asList("project1", "project2");
    var emails = List.of(custodianEmail);
    FlightMap workingMap = new FlightMap();
    workingMap.put(DatasetWorkingMapKeys.SNAPSHOT_GOOGLE_PROJECT_IDS, projectIds);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    assertThat(doOrUndo.apply(flightContext), is(StepResult.getStepResultSuccess()));
    for (var projectId : projectIds) {
      if (grantPolicy) {
        verify(resourceService).assignRolesForSnapshot(projectId, emails);
      } else {
        verify(resourceService).revokeRolesForSnapshot(projectId, emails);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void doStep(boolean inheritSteward) throws Exception {
    SetAuthGcpUserRolesStep step =
        new SetAuthGcpUserRolesStep(resourceService, custodianEmail, inheritSteward);
    verifySetAuth(step::doStep, inheritSteward);
    verifySetAuth(step::undoStep, !inheritSteward);
  }
}

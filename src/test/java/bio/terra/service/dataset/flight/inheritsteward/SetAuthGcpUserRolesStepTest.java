package bio.terra.service.dataset.flight.inheritsteward;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
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
  @Mock private SnapshotService snapshotService;
  @Mock private FlightContext flightContext;

  private final String custodianEmail = "custodianEmail";

  interface DoOrUndo {
    StepResult apply(FlightContext t) throws Exception;
  }

  private void verifySetAuth(DoOrUndo doOrUndo, boolean grantPolicy) throws Exception {
    var snapshots =
        List.of(
            new Snapshot()
                .id(UUID.randomUUID())
                .projectResource(new GoogleProjectResource().googleProjectId("projectId1")),
            new Snapshot()
                .id(UUID.randomUUID())
                .projectResource(new GoogleProjectResource().googleProjectId("projectId2")));
    FlightMap workingMap = new FlightMap();
    workingMap.put(
        DatasetWorkingMapKeys.SNAPSHOT_IDS,
        snapshots.stream().map(Snapshot::getId).collect(Collectors.toList()));
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    for (var snapshot : snapshots) {
      when(snapshotService.retrieve(snapshot.getId())).thenReturn(snapshot);
    }
    assertThat(doOrUndo.apply(flightContext), is(StepResult.getStepResultSuccess()));
    var emails = List.of(custodianEmail);
    for (var snapshot : snapshots) {
      if (grantPolicy) {
        verify(resourceService)
            .assignRolesForSnapshot(snapshot.getProjectResource().getGoogleProjectId(), emails);
      } else {
        verify(resourceService)
            .revokeRolesForSnapshot(snapshot.getProjectResource().getGoogleProjectId(), emails);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void doAndUndoStep(boolean inheritSteward) throws Exception {
    SetAuthGcpUserRolesStep step =
        new SetAuthGcpUserRolesStep(
            resourceService, snapshotService, custodianEmail, inheritSteward);
    verifySetAuth(step::doStep, inheritSteward);
    verifySetAuth(step::undoStep, !inheritSteward);
  }
}

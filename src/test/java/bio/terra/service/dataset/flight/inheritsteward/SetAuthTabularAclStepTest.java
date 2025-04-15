package bio.terra.service.dataset.flight.inheritsteward;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
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
class SetAuthTabularAclStepTest {

  @Mock private BigQuerySnapshotPdao bigQuerySnapshotPdao;
  @Mock private SnapshotService snapshotService;
  @Mock private FlightContext flightContext;

  private final List<String> datasetPolicyEmails = List.of("custodianEmail", "stewardEmail");

  interface DoOrUndo {
    StepResult apply(FlightContext t) throws Exception;
  }

  private void verifySetAuth(SetAuthTabularAclStepTest.DoOrUndo doOrUndo, boolean grantPolicy)
      throws Exception {
    var snapshots =
        List.of(new Snapshot().id(UUID.randomUUID()), new Snapshot().id(UUID.randomUUID()));
    FlightMap workingMap = new FlightMap();
    workingMap.put(
        DatasetWorkingMapKeys.SNAPSHOT_IDS,
        snapshots.stream().map(Snapshot::getId).collect(Collectors.toList()));
    for (var snapshot : snapshots) {
      when(snapshotService.retrieve(snapshot.getId())).thenReturn(snapshot);
    }
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    assertThat(doOrUndo.apply(flightContext), is(StepResult.getStepResultSuccess()));
    for (var snapshot : snapshots) {
      if (grantPolicy) {
        verify(bigQuerySnapshotPdao).grantReadAccessToSnapshot(snapshot, datasetPolicyEmails);
      } else {
        verify(bigQuerySnapshotPdao).revokeReadAccessToSnapshot(snapshot, datasetPolicyEmails);
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void doStep(boolean inheritSteward) throws Exception {
    SetAuthTabularAclStep step =
        new SetAuthTabularAclStep(
            bigQuerySnapshotPdao, snapshotService, datasetPolicyEmails, inheritSteward);
    verifySetAuth(step::doStep, inheritSteward);
    verifySetAuth(step::undoStep, !inheritSteward);
  }
}

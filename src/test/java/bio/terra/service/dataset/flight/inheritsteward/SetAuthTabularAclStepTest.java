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
import bio.terra.stairway.StairwayMapper;
import bio.terra.stairway.StepResult;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeAll;
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

  private final String custodianEmail = "custodianEmail";

  interface DoOrUndo {
    StepResult apply(FlightContext t) throws Exception;
  }

  @BeforeAll
  static void beforeAll() {
    StairwayMapper.getObjectMapper().deactivateDefaultTyping();
  }

  private void verifySetAuth(SetAuthTabularAclStepTest.DoOrUndo doOrUndo, boolean grantPolicy)
      throws Exception {
    var snapshots =
        Arrays.asList(new Snapshot().id(UUID.randomUUID()), new Snapshot().id(UUID.randomUUID()));
    FlightMap workingMap = new FlightMap();
    workingMap.put(
        DatasetWorkingMapKeys.SNAPSHOT_IDS, snapshots.stream().map(Snapshot::getId).toList());
    for (var snapshot : snapshots) {
      when(snapshotService.retrieve(snapshot.getId())).thenReturn(snapshot);
    }
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    assertThat(doOrUndo.apply(flightContext), is(StepResult.getStepResultSuccess()));
    for (var snapshot : snapshots) {
      if (grantPolicy) {
        verify(bigQuerySnapshotPdao).grantReadAccessToSnapshot(snapshot, List.of(custodianEmail));
      } else {
        verify(bigQuerySnapshotPdao).revokeReadAccessToSnapshot(snapshot, List.of(custodianEmail));
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void doStep(boolean inheritSteward) throws Exception {
    SetAuthTabularAclStep step =
        new SetAuthTabularAclStep(
            bigQuerySnapshotPdao, snapshotService, custodianEmail, inheritSteward);
    verifySetAuth(step::doStep, inheritSteward);
    verifySetAuth(step::undoStep, !inheritSteward);
  }
}

package bio.terra.service.snapshot.flight.create;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.dao.CannotSerializeTransactionException;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class CountSnapshotTableRowsStepTest {

  @Mock private BigQuerySnapshotPdao bigQuerySnapshotPdao;
  @Mock private SnapshotDao snapshotDao;
  @Mock private FlightContext flightContext;
  //  private FlightMap workingMap;
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final Snapshot SNAPSHOT =
      new Snapshot().id(SNAPSHOT_ID).name("Snapshot-" + SNAPSHOT_ID);

  private static final SnapshotRequestModel snapshotReq =
      new SnapshotRequestModel().name(SNAPSHOT.getName());

  private Map<String, Long> tableRowCounts;
  private CountSnapshotTableRowsStep step;

  @BeforeEach
  void setup() throws InterruptedException {
    when(snapshotDao.retrieveSnapshotByName(SNAPSHOT.getName())).thenReturn(SNAPSHOT);
    tableRowCounts = Map.of("table", 5L);
    when(bigQuerySnapshotPdao.getSnapshotTableRowCounts(SNAPSHOT)).thenReturn(tableRowCounts);
  }

  @Test
  void testDoStep() throws InterruptedException {
    step = new CountSnapshotTableRowsStep(bigQuerySnapshotPdao, snapshotDao, snapshotReq);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
  }

  @Test
  void testDoStepRetry() throws InterruptedException {
    step = new CountSnapshotTableRowsStep(bigQuerySnapshotPdao, snapshotDao, snapshotReq);
    doThrow(CannotSerializeTransactionException.class)
        .when(snapshotDao)
        .updateSnapshotTableRowCounts(SNAPSHOT, tableRowCounts);

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_RETRY));
  }
}

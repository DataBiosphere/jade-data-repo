package bio.terra.service.snapshot.flight.export;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.tabulardata.google.bigquery.BigQueryExportPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SnapshotExportLoadMappingTableStepTest {

  private static final String FLIGHT_ID = "flightId";
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();

  @Mock private SnapshotService snapshotService;
  @Mock private BigQueryExportPdao bigQueryExportPdao;
  @Mock private FlightContext flightContext;

  private final Snapshot snapshot = new Snapshot().id(SNAPSHOT_ID).name("snapshot");
  private MockedStatic<BigQueryExportPdao> staticPdao;
  private SnapshotExportLoadMappingTableStep step;

  @BeforeEach
  void setUp() {
    staticPdao = mockStatic(BigQueryExportPdao.class);
    FlightMap workingMap = new FlightMap();
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_GSPATHS_FILENAME, "gspaths.json");
    workingMap.put(
        SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_BUCKET, new GoogleBucketResource().name("bucket"));
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(flightContext.getFlightId()).thenReturn(FLIGHT_ID);
    when(snapshotService.retrieve(SNAPSHOT_ID)).thenReturn(snapshot);
    step = new SnapshotExportLoadMappingTableStep(SNAPSHOT_ID, snapshotService, bigQueryExportPdao);
  }

  @AfterEach
  void tearDown() {
    staticPdao.close();
  }

  @Test
  void createsTableAfterDroppingAnyLeftover() throws InterruptedException {
    StepResult result = step.doStep(flightContext);

    assertThat(result.getStepStatus(), is(StepStatus.STEP_RESULT_SUCCESS));
    staticPdao.verify(
        () -> BigQueryExportPdao.deleteFirestoreGsPathExternalTable(snapshot, FLIGHT_ID));
    verify(bigQueryExportPdao)
        .createFirestoreGsPathExternalTable(snapshot, "gs://bucket/gspaths.json", FLIGHT_ID);
  }

  @Test
  void retriesWhenBigQueryDeniesAccess() throws InterruptedException {
    BigQueryException denied =
        new BigQueryException(
            403,
            "Access Denied",
            new BigQueryError("accessDenied", "global", "Access Denied: Bucket bucket"));
    doThrow(denied)
        .when(bigQueryExportPdao)
        .createFirestoreGsPathExternalTable(eq(snapshot), any(), eq(FLIGHT_ID));

    StepResult result = step.doStep(flightContext);

    assertThat(result.getStepStatus(), is(StepStatus.STEP_RESULT_FAILURE_RETRY));
  }

  @Test
  void rethrowsOtherBigQueryErrors() {
    BigQueryException invalid =
        new BigQueryException(400, "Invalid", new BigQueryError("invalid", "global", "Invalid"));
    doThrow(invalid)
        .when(bigQueryExportPdao)
        .createFirestoreGsPathExternalTable(eq(snapshot), any(), eq(FLIGHT_ID));

    assertThrows(BigQueryException.class, () -> step.doStep(flightContext));
  }
}

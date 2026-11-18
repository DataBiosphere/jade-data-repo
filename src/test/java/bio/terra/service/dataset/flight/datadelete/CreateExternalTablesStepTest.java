package bio.terra.service.dataset.flight.datadelete;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.DataDeletionGcsFileModel;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import java.util.ArrayList;
import java.util.List;
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
class CreateExternalTablesStepTest {

  private static final String FLIGHT_ID = "flight-id";
  private static final String SUFFIX = "flight_id";
  private static final String TABLE_NAME = "sample";
  private static final String PATH = "gs://scratch-bucket/rows.csv";
  private static final UUID DATASET_ID = UUID.randomUUID();

  @Mock private BigQueryDatasetPdao bigQueryDatasetPdao;
  @Mock private DatasetService datasetService;
  @Mock private FlightContext flightContext;

  private final Dataset dataset =
      new Dataset().id(DATASET_ID).tables(List.of(new DatasetTable().name(TABLE_NAME)));
  private MockedStatic<BigQueryPdao> staticPdao;
  private CreateExternalTablesStep step;

  @BeforeEach
  void setUp() {
    staticPdao = mockStatic(BigQueryPdao.class);
    FlightMap inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.DATASET_ID.getKeyName(), DATASET_ID.toString());
    FlightMap workingMap = new FlightMap();
    workingMap.put(
        DataDeletionMapKeys.TABLES,
        // Mutable like the list the flight stores, so it round-trips through FlightMap's JSON.
        new ArrayList<>(
            List.of(
                new DataDeletionTableModel()
                    .tableName(TABLE_NAME)
                    .gcsFileSpec(new DataDeletionGcsFileModel().path(PATH)))));
    when(flightContext.getInputParameters()).thenReturn(inputParameters);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(flightContext.getFlightId()).thenReturn(FLIGHT_ID);
    when(datasetService.retrieve(DATASET_ID)).thenReturn(dataset);
    step = new CreateExternalTablesStep(bigQueryDatasetPdao, datasetService);
  }

  @AfterEach
  void tearDown() {
    staticPdao.close();
  }

  @Test
  void createsTablesAfterDroppingAnyLeftover() throws InterruptedException {
    StepResult result = step.doStep(flightContext);

    assertThat(result.getStepStatus(), is(StepStatus.STEP_RESULT_SUCCESS));
    staticPdao.verify(() -> BigQueryPdao.deleteExternalTable(dataset, TABLE_NAME, SUFFIX));
    verify(bigQueryDatasetPdao).createSoftDeleteExternalTable(dataset, PATH, TABLE_NAME, SUFFIX);
  }

  @Test
  void retriesWhenBigQueryDeniesAccess() throws InterruptedException {
    BigQueryException denied =
        new BigQueryException(
            403,
            "Access Denied",
            new BigQueryError("accessDenied", "global", "Access Denied: Bucket scratch-bucket"));
    doThrow(denied)
        .when(bigQueryDatasetPdao)
        .createSoftDeleteExternalTable(eq(dataset), anyString(), eq(TABLE_NAME), any());

    StepResult result = step.doStep(flightContext);

    assertThat(result.getStepStatus(), is(StepStatus.STEP_RESULT_FAILURE_RETRY));
  }

  @Test
  void rethrowsOtherBigQueryErrors() throws InterruptedException {
    BigQueryException invalid =
        new BigQueryException(400, "Invalid", new BigQueryError("invalid", "global", "Invalid"));
    doThrow(invalid)
        .when(bigQueryDatasetPdao)
        .createSoftDeleteExternalTable(eq(dataset), anyString(), eq(TABLE_NAME), any());

    assertThrows(BigQueryException.class, () -> step.doStep(flightContext));
  }
}

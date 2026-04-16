package bio.terra.service.dataset.flight.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.PdaoLoadStatistics;
import bio.terra.common.category.Unit;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.IngestFailureException;
import bio.terra.service.tabulardata.google.bigquery.BigQueryDatasetPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class IngestLoadTableStepTest {

  @Mock private DatasetService datasetService;
  @Mock private BigQueryDatasetPdao bigQueryDatasetPdao;
  @Mock private FlightContext flightContext;
  @Mock private FlightMap workingMap;

  private MockedStatic<IngestUtils> mockedUtils;
  private IngestLoadTableStep step;

  private Dataset dataset;
  private DatasetTable datasetTable;
  private IngestRequestModel ingestRequest;

  @BeforeEach
  void setUp() {
    mockedUtils = mockStatic(IngestUtils.class);
    step = new IngestLoadTableStep(datasetService, bigQueryDatasetPdao);

    // Setup common test data
    dataset = new Dataset();
    dataset.id(UUID.randomUUID());
    dataset.name("test_dataset");

    datasetTable = new DatasetTable();
    datasetTable.name("test_table");

    ingestRequest = new IngestRequestModel();
    ingestRequest.table("test_table");
    ingestRequest.format(IngestRequestModel.FormatEnum.JSON);
  }

  private void setupDoStepMocks() {
    // Setup mocks needed for doStep tests
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(IngestUtils.getDataset(flightContext, datasetService)).thenReturn(dataset);
    when(IngestUtils.getDatasetTable(flightContext, dataset)).thenReturn(datasetTable);
    when(IngestUtils.getStagingTableName(flightContext)).thenReturn("staging_table");
    when(IngestUtils.getIngestRequestModel(flightContext)).thenReturn(ingestRequest);
    when(workingMap.get(eq(IngestMapKeys.INGEST_CONTROL_FILE_PATH), eq(String.class)))
        .thenReturn("gs://test-bucket/test-file.json");
  }

  private void setupUndoStepMocks() {
    // Setup mocks needed for undoStep tests
    when(IngestUtils.getDataset(flightContext, datasetService)).thenReturn(dataset);
    when(IngestUtils.getStagingTableName(flightContext)).thenReturn("staging_table");
  }

  @AfterEach
  void tearDown() {
    mockedUtils.close();
  }

  @Test
  void testDoStep_success_noBadRecords() throws InterruptedException {
    // Arrange
    setupDoStepMocks();
    ingestRequest.maxBadRecords(0);
    PdaoLoadStatistics stats = new PdaoLoadStatistics(0, 100, Instant.now(), Instant.now());

    when(bigQueryDatasetPdao.loadToStagingTable(
            eq(dataset), eq(datasetTable), eq("staging_table"), eq(ingestRequest), anyString()))
        .thenReturn(stats);

    // Act
    StepResult result = step.doStep(flightContext);

    // Assert
    assertTrue(result.isSuccess());
    mockedUtils.verify(() -> IngestUtils.putIngestStatistics(flightContext, stats));
  }

  @Test
  void testDoStep_success_withinBadRecordsThreshold() throws InterruptedException {
    // Arrange
    setupDoStepMocks();
    ingestRequest.maxBadRecords(10);
    PdaoLoadStatistics stats = new PdaoLoadStatistics(5, 100, Instant.now(), Instant.now());

    when(bigQueryDatasetPdao.loadToStagingTable(
            eq(dataset), eq(datasetTable), eq("staging_table"), eq(ingestRequest), anyString()))
        .thenReturn(stats);

    // Act
    StepResult result = step.doStep(flightContext);

    // Assert
    assertTrue(result.isSuccess());
    mockedUtils.verify(() -> IngestUtils.putIngestStatistics(flightContext, stats));
  }

  @Test
  void testDoStep_success_nullMaxBadRecords() throws InterruptedException {
    // Arrange - maxBadRecords is null (no limit)
    setupDoStepMocks();
    ingestRequest.maxBadRecords(null);
    PdaoLoadStatistics stats = new PdaoLoadStatistics(100, 100, Instant.now(), Instant.now());

    when(bigQueryDatasetPdao.loadToStagingTable(
            eq(dataset), eq(datasetTable), eq("staging_table"), eq(ingestRequest), anyString()))
        .thenReturn(stats);

    // Act
    StepResult result = step.doStep(flightContext);

    // Assert - should succeed even with many bad records when maxBadRecords is null
    assertTrue(result.isSuccess());
    mockedUtils.verify(() -> IngestUtils.putIngestStatistics(flightContext, stats));
  }

  @ParameterizedTest
  @CsvSource({
    "0, 1", // maxBadRecords=0, badRecords=1 - should fail
    "0, 5", // maxBadRecords=0, badRecords=5 - should fail (user-reported case)
    "5, 6", // maxBadRecords=5, badRecords=6 - should fail
    "10, 11", // maxBadRecords=10, badRecords=11 - should fail
    "100, 101" // maxBadRecords=100, badRecords=101 - should fail
  })
  void testDoStep_failure_exceedsMaxBadRecords(int maxBadRecords, int badRecords)
      throws InterruptedException {
    // Arrange
    setupDoStepMocks();
    ingestRequest.maxBadRecords(maxBadRecords);
    PdaoLoadStatistics stats =
        new PdaoLoadStatistics(badRecords, 100, Instant.now(), Instant.now());

    when(bigQueryDatasetPdao.loadToStagingTable(
            eq(dataset), eq(datasetTable), eq("staging_table"), eq(ingestRequest), anyString()))
        .thenReturn(stats);

    // Act
    StepResult result = step.doStep(flightContext);

    // Assert - should return failure result
    assertEquals(
        StepStatus.STEP_RESULT_FAILURE_FATAL,
        result.getStepStatus(),
        "Step should fail when bad records exceed threshold");
    assertTrue(result.getException().isPresent(), "Result should contain exception");

    IngestFailureException exception = (IngestFailureException) result.getException().get();
    String expectedMessage = String.format("Failed to load data into dataset %s", dataset.getId());
    assertEquals(expectedMessage, exception.getMessage());

    // Verify error details
    assertTrue(exception.getCauses().size() >= 1);
    String errorDetail = exception.getCauses().get(0);
    assertTrue(
        errorDetail.contains(String.format("%d records failed to ingest", badRecords)),
        "Error should mention bad record count");
    assertTrue(
        errorDetail.contains(String.format("%d allowed failed records", maxBadRecords)),
        "Error should mention max allowed");

    // Verify statistics were still saved before failure
    mockedUtils.verify(() -> IngestUtils.putIngestStatistics(flightContext, stats));
  }

  @Test
  void testDoStep_failure_maxBadRecordsZero_withBadRecords() throws InterruptedException {
    // Arrange - This is the specific case reported by users
    setupDoStepMocks();
    ingestRequest.maxBadRecords(0);
    PdaoLoadStatistics stats = new PdaoLoadStatistics(5, 5, Instant.now(), Instant.now());

    when(bigQueryDatasetPdao.loadToStagingTable(
            eq(dataset), eq(datasetTable), eq("staging_table"), eq(ingestRequest), anyString()))
        .thenReturn(stats);

    // Act
    StepResult result = step.doStep(flightContext);

    // Assert - should return failure result
    assertEquals(
        StepStatus.STEP_RESULT_FAILURE_FATAL,
        result.getStepStatus(),
        "Step should fail when bad records exceed threshold");
    assertTrue(result.getException().isPresent(), "Result should contain exception");

    IngestFailureException exception = (IngestFailureException) result.getException().get();
    assertTrue(exception.getMessage().contains("Failed to load data into dataset"));
    assertTrue(
        exception.getCauses().get(0).contains("5 records failed to ingest"),
        "Should mention 5 failed records");
    assertTrue(
        exception.getCauses().get(0).contains("0 allowed failed records"),
        "Should mention 0 allowed");
  }

  @ParameterizedTest
  @CsvSource({
    "5, 5", // Exactly at threshold - should succeed
    "10, 10", // Exactly at threshold - should succeed
    "0, 0" // No bad records with zero tolerance - should succeed
  })
  void testDoStep_success_exactlyAtThreshold(int maxBadRecords, int badRecords)
      throws InterruptedException {
    // Arrange
    setupDoStepMocks();
    ingestRequest.maxBadRecords(maxBadRecords);
    PdaoLoadStatistics stats =
        new PdaoLoadStatistics(badRecords, 100, Instant.now(), Instant.now());

    when(bigQueryDatasetPdao.loadToStagingTable(
            eq(dataset), eq(datasetTable), eq("staging_table"), eq(ingestRequest), anyString()))
        .thenReturn(stats);

    // Act
    StepResult result = step.doStep(flightContext);

    // Assert - should succeed when badRecords equals maxBadRecords
    assertTrue(result.isSuccess());
    mockedUtils.verify(() -> IngestUtils.putIngestStatistics(flightContext, stats));
  }

  @Test
  void testUndoStep_deleteStagingTable() throws InterruptedException {
    // Arrange
    setupUndoStepMocks();

    // Act
    StepResult result = step.undoStep(flightContext);

    // Assert
    assertTrue(result.isSuccess());
    verify(bigQueryDatasetPdao).deleteDatasetTable(dataset, "staging_table");
  }

  @Test
  void testDoStep_verifiesLoadToStagingTableCalledWithCorrectParams() throws InterruptedException {
    // Arrange
    setupDoStepMocks();
    ingestRequest.maxBadRecords(0);
    PdaoLoadStatistics stats = new PdaoLoadStatistics(0, 100, Instant.now(), Instant.now());
    String controlFilePath = "gs://test-bucket/test-file.json";

    when(bigQueryDatasetPdao.loadToStagingTable(
            eq(dataset), eq(datasetTable), eq("staging_table"), eq(ingestRequest), anyString()))
        .thenReturn(stats);

    // Act
    step.doStep(flightContext);

    // Assert - verify the PDAO was called with correct parameters
    verify(bigQueryDatasetPdao)
        .loadToStagingTable(dataset, datasetTable, "staging_table", ingestRequest, controlFilePath);
  }
}

package bio.terra.service.filedata.flight.ingest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import bio.terra.common.CollectionType;
import bio.terra.common.Column;
import bio.terra.common.category.Unit;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.TableDataType;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.dataset.exception.InvalidFileRefException;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.dataset.flight.ingest.IngestValidateScratchTableFilerefsStep;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.tables.TableDirectoryDao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.azure.data.tables.TableServiceClient;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class IngestValidateScratchTableFilerefsStepTest {
  @Mock private AzureAuthService azureAuthService;
  @Mock private DatasetService datasetService;
  @Mock private AzureSynapsePdao azureSynapsePdao;
  @Mock private TableDirectoryDao tableDirectoryDao;
  @Mock private TableServiceClient tableServiceClient;
  @Mock private FlightContext flightContext;
  private static final String FLIGHT_ID = "a_flight_id";
  private static final String SCRATCH_TABLE_NAME =
      IngestUtils.getSynapseScratchTableName(FLIGHT_ID);
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final String TABLE_NAME = "table_to_populate";
  private static final Column STRING_COLUMN =
      new Column().name("a_string_column").type(TableDataType.STRING);
  private static final Column FILEREF_COLUMN =
      new Column().name("a_fileref_column").type(TableDataType.FILEREF);
  private static final String INVALID_REFID = "invalid_refid";
  private static final String VALID_REFID = "valid_refid";

  @BeforeEach
  void setup() {
    AzureStorageAuthInfo datasetStorageAuthInfo =
        new AzureStorageAuthInfo(UUID.randomUUID(), "rg_name", "sa_name");

    when(flightContext.getFlightId()).thenReturn(FLIGHT_ID);

    FlightMap inputParameters = new FlightMap();
    inputParameters.put(
        JobMapKeys.REQUEST.getKeyName(), new IngestRequestModel().table(TABLE_NAME));
    inputParameters.put(JobMapKeys.DATASET_ID.getKeyName(), DATASET_ID);
    when(flightContext.getInputParameters()).thenReturn(inputParameters);

    FlightMap workingMap = new FlightMap();
    workingMap.put(CommonMapKeys.DATASET_STORAGE_AUTH_INFO, datasetStorageAuthInfo);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);

    when(azureAuthService.getTableServiceClient(datasetStorageAuthInfo))
        .thenReturn(tableServiceClient);
  }

  /** Mock dataset retrieval to include a single table with the specified columns. */
  private void mockDatasetWithColumns(Column... columns) {
    DatasetTable table = new DatasetTable().name(TABLE_NAME).columns(Arrays.asList(columns));
    Dataset dataset = new Dataset().id(DATASET_ID).tables(List.of(table));
    when(datasetService.retrieveForIngest(DATASET_ID)).thenReturn(dataset);
  }

  /**
   * Verify that we obtained fileref IDs from our fileref column in our scratch table, and only our
   * fileref column.
   */
  private void verifyFilerefIdFetch() {
    verify(azureSynapsePdao)
        .getRefIds(
            SCRATCH_TABLE_NAME, Column.toSynapseColumn(FILEREF_COLUMN), CollectionType.DATASET);
    verifyNoMoreInteractions(azureSynapsePdao);
  }

  @Test
  void doStep_noFilerefColumns_success() throws InterruptedException {
    mockDatasetWithColumns(STRING_COLUMN);

    IngestValidateScratchTableFilerefsStep step =
        new IngestValidateScratchTableFilerefsStep(
            azureAuthService, datasetService, azureSynapsePdao, tableDirectoryDao);

    StepResult stepResult = step.doStep(flightContext);
    assertThat(
        "An ingest to a table with no fileref columns succeeds",
        stepResult.getStepStatus(),
        equalTo(StepStatus.STEP_RESULT_SUCCESS));

    // Our scratch table has no fileref columns, so the step does not have anything to check.
    verifyNoInteractions(azureSynapsePdao);
  }

  @Test
  void doStep_validFileref_success() throws InterruptedException {
    mockDatasetWithColumns(STRING_COLUMN, FILEREF_COLUMN);

    List<String> filerefs = List.of(VALID_REFID);
    when(azureSynapsePdao.getRefIds(
            SCRATCH_TABLE_NAME, Column.toSynapseColumn(FILEREF_COLUMN), CollectionType.DATASET))
        .thenReturn(filerefs);
    when(tableDirectoryDao.validateRefIds(tableServiceClient, DATASET_ID, filerefs))
        .thenReturn(List.of());

    IngestValidateScratchTableFilerefsStep step =
        new IngestValidateScratchTableFilerefsStep(
            azureAuthService, datasetService, azureSynapsePdao, tableDirectoryDao);

    StepResult stepResult = step.doStep(flightContext);
    assertThat(
        "Step succeeds when all filerefs in the scratch table are valid",
        stepResult.getStepStatus(),
        equalTo(StepStatus.STEP_RESULT_SUCCESS));

    verifyFilerefIdFetch();
  }

  private static Stream<List<String>> doStep_invalidFileref_throwsInvalidFileRefException() {
    return Stream.of(
        List.of(INVALID_REFID), // Only an invalid fileref
        List.of(VALID_REFID, INVALID_REFID) // Both valid and invalid filerefs
        );
  }

  @ParameterizedTest
  @MethodSource
  void doStep_invalidFileref_throwsInvalidFileRefException(List<String> filerefs) {
    mockDatasetWithColumns(STRING_COLUMN, FILEREF_COLUMN);

    when(azureSynapsePdao.getRefIds(
            SCRATCH_TABLE_NAME, Column.toSynapseColumn(FILEREF_COLUMN), CollectionType.DATASET))
        .thenReturn(filerefs);
    when(tableDirectoryDao.validateRefIds(tableServiceClient, DATASET_ID, filerefs))
        .thenReturn(List.of(INVALID_REFID));

    IngestValidateScratchTableFilerefsStep step =
        new IngestValidateScratchTableFilerefsStep(
            azureAuthService, datasetService, azureSynapsePdao, tableDirectoryDao);

    InvalidFileRefException exception =
        assertThrows(
            InvalidFileRefException.class,
            () -> step.doStep(flightContext),
            "Step throws when 1+ filerefs in the scratch table are invalid");
    List<String> actualInvalidFilerefs = exception.getCauses();
    assertThat("Exactly one fileref is invalid", actualInvalidFilerefs, hasSize(1));
    assertThat(
        "Our expected invalid fileref is recorded as a failure cause",
        actualInvalidFilerefs.get(0),
        containsString(INVALID_REFID));

    verifyFilerefIdFetch();
  }
}

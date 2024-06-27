package bio.terra.service.dataset.flight.ingest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class IngestCreateParquetFilesStepTest {
  @Mock private AzureSynapsePdao azureSynapsePdao;
  @Mock private AzureBlobStorePdao azureBlobStorePdao;
  @Mock private DatasetService datasetService;

  @Mock private FlightContext flightContext;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private IngestCreateParquetFilesStep step;
  private FlightMap flightMap;
  private FlightMap inputParameters;

  private static final String FLIGHT_ID = "flightId";
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final String PARQUET_FILE_PATH = "parquetFilePath";
  private static final String DATASET_TABLE_NAME = "datasetTableName";

  private static final UUID SUBSCRIPTION_ID = UUID.randomUUID();
  private static final String RESOURCE_GROUP_NAME = "resourceGroupName";
  private static final String APPLICATION_DEPLOYMENT_ID =
      String.format("subscriptions/%s/resourceGroups/%s", SUBSCRIPTION_ID, RESOURCE_GROUP_NAME);

  @BeforeEach
  public void setup() {
    step =
        new IngestCreateParquetFilesStep(
            azureSynapsePdao, azureBlobStorePdao, datasetService, TEST_USER);

    IngestRequestModel ingestRequestModel =
        new IngestRequestModel()
            .table(DATASET_TABLE_NAME)
            .path(PARQUET_FILE_PATH)
            .format(FormatEnum.CSV);
    DatasetTable datasetTable = new DatasetTable().name(DATASET_TABLE_NAME);
    Dataset dataset =
        new Dataset().name(DATASET_ID.toString()).id(DATASET_ID).tables(List.of(datasetTable));
    AzureApplicationDeploymentResource applicationResource =
        new AzureApplicationDeploymentResource()
            .azureApplicationDeploymentId(APPLICATION_DEPLOYMENT_ID);
    AzureStorageAccountResource storageAccountResource =
        new AzureStorageAccountResource().applicationResource(applicationResource);

    flightMap = new FlightMap();
    flightMap.put(IngestMapKeys.PARQUET_FILE_PATH, PARQUET_FILE_PATH);
    flightMap.put(IngestMapKeys.AZURE_ROWS_FAILED_VALIDATION, 0);
    flightMap.put(CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE, storageAccountResource);

    inputParameters = new FlightMap();
    inputParameters.put(JobMapKeys.DATASET_ID.getKeyName(), DATASET_ID.toString());
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), ingestRequestModel);

    when(flightContext.getWorkingMap()).thenReturn(flightMap);
    when(flightContext.getInputParameters()).thenReturn(inputParameters);
    when(flightContext.getFlightId()).thenReturn(FLIGHT_ID);
    when(datasetService.retrieve(DATASET_ID)).thenReturn(dataset);
  }

  @Test
  void testDoAndUndoStepSuccess() throws InterruptedException, SQLException {
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(azureSynapsePdao).createFinalParquetFiles(any(), any(), any(), any(), any());
    StepResult undoResult = step.undoStep(flightContext);
    verify(azureSynapsePdao).dropTables(any());
    verify(azureBlobStorePdao).deleteMetadataParquet(any(), any(), any());
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
  }

  @Test
  void testDoStepSQLException() throws InterruptedException, SQLException {
    when(azureSynapsePdao.createFinalParquetFiles(any(), any(), any(), any(), any()))
        .thenThrow(SQLException.class);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));
  }

  @Test
  void testDoStepWithCombinedFailedRowCount() throws InterruptedException {
    flightMap.put(IngestMapKeys.COMBINED_FAILED_ROW_COUNT, 10);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
  }

  @Test
  void testDoStepWithCombinedFileIngest() throws InterruptedException {
    flightMap.put(IngestMapKeys.NUM_BULK_LOAD_FILE_MODELS, 1);
    flightMap.put(IngestMapKeys.BULK_LOAD_RESULT, new BulkLoadArrayResultModel());
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
  }

  @Test
  void testDoStepWithPayloadIngest() throws InterruptedException {
    // Override the IngestRequestModel using the ARRAY format
    IngestRequestModel ingestRequestModel =
        new IngestRequestModel()
            .table(DATASET_TABLE_NAME)
            .path(PARQUET_FILE_PATH)
            .format(FormatEnum.ARRAY);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), ingestRequestModel);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
  }
}

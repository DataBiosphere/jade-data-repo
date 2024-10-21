package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DataDeletionGcsFileModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.CloudFileReader;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.google.cloud.storage.StorageException;
import java.util.List;
import org.apache.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class ValidateBucketAccessStepTest {
  @Mock private CloudFileReader cloudFileReader;
  @Mock private Dataset dataset;
  @Mock private FlightContext flightContext;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final String PROJECT_ID = "googleProjectId";

  private ValidateBucketAccessStep step;
  private FlightMap inputParameters;

  @BeforeEach
  void setup() {
    step = new ValidateBucketAccessStep(cloudFileReader, TEST_USER, dataset);
    inputParameters = new FlightMap();
    when(flightContext.getInputParameters()).thenReturn(inputParameters);
  }

  /**
   * @param cloudPlatform destination dataset's cloud platform
   * @return expected Google project ID for the specified cloud platform
   */
  private String mockProjectResourceAndReturnExpectedProjectId(CloudPlatform cloudPlatform) {
    var wrapper = CloudPlatformWrapper.of(cloudPlatform);
    if (wrapper.isGcp()) {
      when(dataset.getProjectResource())
          .thenReturn(new GoogleProjectResource().googleProjectId(PROJECT_ID));
      return PROJECT_ID;
    } else {
      when(dataset.getProjectResource()).thenReturn(null);
      return null;
    }
  }

  @ParameterizedTest
  @EnumSource(names = {"GCP", "AZURE"})
  void testValidateSingleFileIngest(CloudPlatform cloudPlatform) throws InterruptedException {
    var projectId = mockProjectResourceAndReturnExpectedProjectId(cloudPlatform);
    var sourcePath = "singleFileSourcePath";
    var request = new FileLoadModel().sourcePath(sourcePath);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);

    assertThat(step.doStep(flightContext).getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(cloudFileReader).validateUserCanRead(List.of(sourcePath), projectId, TEST_USER, dataset);
  }

  @ParameterizedTest
  @EnumSource(names = {"GCP", "AZURE"})
  void testValidateBulkFileJsonIngest(CloudPlatform cloudPlatform) throws InterruptedException {
    var projectId = mockProjectResourceAndReturnExpectedProjectId(cloudPlatform);
    var controlPath = "bulkFileControlPath";
    var request = new BulkLoadRequestModel().loadControlFile(controlPath);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);

    assertThat(step.doStep(flightContext).getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(cloudFileReader)
        .validateUserCanRead(List.of(controlPath), projectId, TEST_USER, dataset);
  }

  @ParameterizedTest
  @EnumSource(names = {"GCP", "AZURE"})
  void testValidateBulkFileArrayIngest(CloudPlatform cloudPlatform) throws InterruptedException {
    var projectId = mockProjectResourceAndReturnExpectedProjectId(cloudPlatform);
    List<String> sourcePaths = List.of("a", "b", "c");

    var files = sourcePaths.stream().map(this::createBulkFileLoadModel).toList();
    var request = new BulkLoadArrayRequestModel().loadArray(files);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);

    assertThat(step.doStep(flightContext).getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(cloudFileReader).validateUserCanRead(sourcePaths, projectId, TEST_USER, dataset);
  }

  private BulkLoadFileModel createBulkFileLoadModel(String sourcePath) {
    return new BulkLoadFileModel().sourcePath(sourcePath);
  }

  @ParameterizedTest
  @EnumSource(names = {"GCP", "AZURE"})
  void testValidateCombinedArrayIngest(CloudPlatform cloudPlatform) throws InterruptedException {
    var projectId = mockProjectResourceAndReturnExpectedProjectId(cloudPlatform);
    var controlPath = "tdrGeneratedArrayControlPath";
    var request = new IngestRequestModel().format(FormatEnum.ARRAY).path(controlPath);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);

    assertThat(step.doStep(flightContext).getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(cloudFileReader).validateUserCanRead(List.of(), projectId, TEST_USER, dataset);
  }

  @ParameterizedTest
  @EnumSource(names = {"GCP", "AZURE"})
  void testValidateCombinedJsonIngest(CloudPlatform cloudPlatform) throws InterruptedException {
    var projectId = mockProjectResourceAndReturnExpectedProjectId(cloudPlatform);
    var controlPath = "userProvidedJsonControlPath";
    var request = new IngestRequestModel().format(FormatEnum.JSON).path(controlPath);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);

    assertThat(step.doStep(flightContext).getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(cloudFileReader)
        .validateUserCanRead(List.of(controlPath), projectId, TEST_USER, dataset);
  }

  @ParameterizedTest
  @EnumSource(names = {"GCP", "AZURE"})
  void testValidateCombinedCsvIngest(CloudPlatform cloudPlatform) throws InterruptedException {
    var projectId = mockProjectResourceAndReturnExpectedProjectId(cloudPlatform);
    var controlPath = "userProvidedCsvControlPath";
    var request = new IngestRequestModel().format(FormatEnum.CSV).path(controlPath);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);

    assertThat(step.doStep(flightContext).getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(cloudFileReader)
        .validateUserCanRead(List.of(controlPath), projectId, TEST_USER, dataset);
  }

  @ParameterizedTest
  @EnumSource(names = {"GCP", "AZURE"})
  void testValidateSoftDeletes(CloudPlatform cloudPlatform) throws InterruptedException {
    var projectId = mockProjectResourceAndReturnExpectedProjectId(cloudPlatform);
    List<String> paths = List.of("a", "b", "c");

    var tables = paths.stream().map(this::createDataDeletionTableModel).toList();
    var request = new DataDeletionRequest().tables(tables);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);

    assertThat(step.doStep(flightContext).getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(cloudFileReader).validateUserCanRead(paths, projectId, TEST_USER, dataset);
  }

  private DataDeletionTableModel createDataDeletionTableModel(String path) {
    return new DataDeletionTableModel().gcsFileSpec(new DataDeletionGcsFileModel().path(path));
  }

  @Test
  void testStepThrowsOnUnhandledRequestType() {
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), "Strings are not supported request types");
    assertThrows(IllegalArgumentException.class, () -> step.doStep(flightContext));
  }

  @ParameterizedTest
  @EnumSource(names = {"GCP", "AZURE"})
  void testStepFailsOnGeneralStorageException(CloudPlatform cloudPlatform)
      throws InterruptedException {
    var projectId = mockProjectResourceAndReturnExpectedProjectId(cloudPlatform);
    var sourcePath = "singleFileSourcePath";
    var request = new FileLoadModel().sourcePath(sourcePath);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);

    var retryException =
        new StorageException(
            HttpStatus.SC_FORBIDDEN, ValidateBucketAccessStep.PET_PROPAGATION_ERROR_MSG);
    var fatalException = new StorageException(HttpStatus.SC_FORBIDDEN, "fatal");

    doThrow(retryException, fatalException)
        .when(cloudFileReader)
        .validateUserCanRead(List.of(sourcePath), projectId, TEST_USER, dataset);

    // First attempt: retry exception
    StepResult retryResult = step.doStep(flightContext);
    assertThat(retryResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_RETRY));
    verify(cloudFileReader).validateUserCanRead(List.of(sourcePath), projectId, TEST_USER, dataset);

    // Second attempt: fatal exception
    StepResult fatalResult = step.doStep(flightContext);
    assertThat(fatalResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));
    verify(cloudFileReader, times(2))
        .validateUserCanRead(List.of(sourcePath), projectId, TEST_USER, dataset);
  }
}

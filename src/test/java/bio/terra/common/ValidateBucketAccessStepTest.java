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
import bio.terra.model.DataDeletionGcsFileModel;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.model.FileLoadModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.IngestRequestModel.FormatEnum;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.google.cloud.storage.StorageException;
import java.util.List;
import org.apache.http.HttpStatus;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.context.ActiveProfiles;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@ActiveProfiles({"unittest"})
@Category(Unit.class)
public class ValidateBucketAccessStepTest {
  @Mock private GcsPdao gcsPdao;
  @Mock private Dataset dataset;
  @Mock private FlightContext flightContext;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final String PROJECT_ID = "googleProjectId";

  private ValidateBucketAccessStep step;
  private FlightMap inputParameters;

  @Before
  public void setup() {
    step = new ValidateBucketAccessStep(gcsPdao, TEST_USER, dataset);
    inputParameters = new FlightMap();

    when(dataset.getProjectResource())
        .thenReturn(new GoogleProjectResource().googleProjectId(PROJECT_ID));
    when(flightContext.getInputParameters()).thenReturn(inputParameters);
  }

  @Test
  public void testValidateSingleFileIngest() throws InterruptedException {
    var sourcePath = "singleFileSourcePath";
    var request = new FileLoadModel().sourcePath(sourcePath);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(gcsPdao).validateUserCanRead(List.of(sourcePath), PROJECT_ID, TEST_USER, dataset);
  }

  @Test
  public void testValidateBulkFileJsonIngest() throws InterruptedException {
    var controlPath = "bulkFileControlPath";
    var request = new BulkLoadRequestModel().loadControlFile(controlPath);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(gcsPdao).validateUserCanRead(List.of(controlPath), PROJECT_ID, TEST_USER, dataset);
  }

  @Test
  public void testValidateBulkFileArrayIngest() throws InterruptedException {
    var sourcePaths = List.of("a", "b", "c");
    var files = sourcePaths.stream().map(this::createBulkFileLoadModel).toList();
    var request = new BulkLoadArrayRequestModel().loadArray(files);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(gcsPdao).validateUserCanRead(sourcePaths, PROJECT_ID, TEST_USER, dataset);
  }

  private BulkLoadFileModel createBulkFileLoadModel(String sourcePath) {
    return new BulkLoadFileModel().sourcePath(sourcePath);
  }

  @Test
  public void testValidateCombinedArrayIngest() throws InterruptedException {
    var controlPath = "tdrGeneratedArrayControlPath";
    var request = new IngestRequestModel().format(FormatEnum.ARRAY).path(controlPath);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(gcsPdao).validateUserCanRead(List.of(), PROJECT_ID, TEST_USER, dataset);
  }

  @Test
  public void testValidateCombinedJsonIngest() throws InterruptedException {
    var controlPath = "userProvidedJsonControlPath";
    var request = new IngestRequestModel().format(FormatEnum.JSON).path(controlPath);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(gcsPdao).validateUserCanRead(List.of(controlPath), PROJECT_ID, TEST_USER, dataset);
  }

  @Test
  public void testValidateCombinedCsvIngest() throws InterruptedException {
    var controlPath = "userProvidedCsvControlPath";
    var request = new IngestRequestModel().format(FormatEnum.CSV).path(controlPath);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(gcsPdao).validateUserCanRead(List.of(controlPath), PROJECT_ID, TEST_USER, dataset);
  }

  @Test
  public void testValidateSoftDeletes() throws InterruptedException {
    var paths = List.of("a", "b", "c");
    var tables = paths.stream().map(this::createDataDeletionTableModel).toList();
    var request = new DataDeletionRequest().tables(tables);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(gcsPdao).validateUserCanRead(paths, PROJECT_ID, TEST_USER, dataset);
  }

  private DataDeletionTableModel createDataDeletionTableModel(String path) {
    return new DataDeletionTableModel().gcsFileSpec(new DataDeletionGcsFileModel().path(path));
  }

  @Test
  public void testStepThrowsOnUnhandledRequestType() {
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), "Strings are not supported request types");
    assertThrows(IllegalArgumentException.class, () -> step.doStep(flightContext));
  }

  @Test
  public void testStepFailsOnGeneralStorageException() throws InterruptedException {
    var sourcePath = "singleFileSourcePath";
    var request = new FileLoadModel().sourcePath(sourcePath);
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), request);

    var retryException =
        new StorageException(
            HttpStatus.SC_FORBIDDEN, ValidateBucketAccessStep.PET_PROPAGATION_ERROR_MSG);
    var fatalException = new StorageException(HttpStatus.SC_FORBIDDEN, "fatal");

    doThrow(retryException, fatalException)
        .when(gcsPdao)
        .validateUserCanRead(List.of(sourcePath), PROJECT_ID, TEST_USER, dataset);

    // First attempt: retry exception
    StepResult retryResult = step.doStep(flightContext);
    assertThat(retryResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_RETRY));
    verify(gcsPdao).validateUserCanRead(List.of(sourcePath), PROJECT_ID, TEST_USER, dataset);

    // Second attempt: fatal exception
    StepResult fatalResult = step.doStep(flightContext);
    assertThat(fatalResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));
    verify(gcsPdao, times(2))
        .validateUserCanRead(List.of(sourcePath), PROJECT_ID, TEST_USER, dataset);
  }
}

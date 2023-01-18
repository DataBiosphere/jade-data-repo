package bio.terra.service.filedata.flight.ingest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.exception.PdaoFileCopyException;
import bio.terra.model.FileLoadModel;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.FileIdService;
import bio.terra.service.filedata.exception.GoogleInternalServerErrorException;
import bio.terra.service.filedata.exception.InvalidUserProjectException;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class IngestFilePrimaryDataStepTest extends TestCase {

  private static final UUID RANDOM_FILE_ID = UUID.randomUUID();

  private static final FileLoadModel FILE_LOAD_MODEL =
      new FileLoadModel()
          .loadTag("lt")
          .sourcePath("gs://bucket/path/file.txt")
          .targetPath("/foo/bar/baz.txt");

  @MockBean private GcsPdao gcsPdao;

  @MockBean private Dataset dataset;

  @MockBean private ConfigurationService configService;

  @SpyBean private FileIdService fileIdService;

  private IngestFilePrimaryDataStep step;

  private FlightContext flightContext;

  @Before
  public void setup() {
    step = new IngestFilePrimaryDataStep(dataset, gcsPdao, configService);
    flightContext = mock(FlightContext.class);

    FlightMap inputParameters = new FlightMap();
    inputParameters.put(
        FileMapKeys.BUCKET_INFO, new GoogleBucketResource().resourceId(UUID.randomUUID()));
    inputParameters.put(JobMapKeys.REQUEST.getKeyName(), FILE_LOAD_MODEL);
    when(flightContext.getInputParameters()).thenReturn(inputParameters);

    FlightMap workingMap = new FlightMap();
    workingMap.put(FileMapKeys.FILE_ID, RANDOM_FILE_ID.toString());
    when(flightContext.getWorkingMap()).thenReturn(workingMap);

    GoogleProjectResource projectResource =
        new GoogleProjectResource().googleProjectId("googleProjectId");
    when(dataset.getProjectResource()).thenReturn(projectResource);
  }

  @Test
  public void testDoStepSelfHostedRetryThenSucceed() {
    // Dataset is self-hosted
    when(dataset.isSelfHosted()).thenReturn(true);
    // Step throws two retryable exceptions then succeeds
    FSFileInfo fileInfo = mock(FSFileInfo.class);
    when(gcsPdao.linkSelfHostedFile(any(), any(), any()))
        .thenThrow(new InvalidUserProjectException("retryable"))
        .thenThrow(new GoogleInternalServerErrorException("retryable"))
        .thenReturn(fileInfo);

    // 1 - throws retryable "InvalidUserProjectException" exception
    StepResult result = step.doStep(flightContext);
    verify(gcsPdao, times(1)).linkSelfHostedFile(any(), any(), any());
    assertThat(
        "Step failed due to InvalidUserProjectException and should be retried",
        StepStatus.STEP_RESULT_FAILURE_RETRY,
        equalTo(result.getStepStatus()));

    // 2 - throws retryable "GoogleInternalServerErrorException" exception
    result = step.doStep(flightContext);
    verify(gcsPdao, times(2)).linkSelfHostedFile(any(), any(), any());
    assertThat(
        "Step failed due to GoogleInternalServerErrorException and should be retried",
        StepStatus.STEP_RESULT_FAILURE_RETRY,
        equalTo(result.getStepStatus()));

    // 3 - Succeeds
    result = step.doStep(flightContext);
    verify(gcsPdao, times(3)).linkSelfHostedFile(any(), any(), any());
    assertThat(
        "Retried step succeeds", StepStatus.STEP_RESULT_SUCCESS, equalTo(result.getStepStatus()));
  }

  @Test
  public void testDoStepExternallyHostedRetryThenSucceed() {
    // Dataset is externally hosted by default
    FSFileInfo fileInfo = mock(FSFileInfo.class);
    // Step throws two retryable exceptions then succeeds
    when(gcsPdao.copyFile(any(), any(), any(), any()))
        .thenThrow(new InvalidUserProjectException("retryable"))
        .thenThrow(new GoogleInternalServerErrorException("retryable"))
        .thenReturn(fileInfo);

    // 1 - throws retryable "InvalidUserProjectException" exception
    StepResult result = step.doStep(flightContext);
    verify(gcsPdao, times(1)).copyFile(any(), any(), any(), any());
    assertThat(
        "Step failed due to InvalidUserProjectException and should be retried",
        StepStatus.STEP_RESULT_FAILURE_RETRY,
        equalTo(result.getStepStatus()));

    // 2 - throws retryable "GoogleInternalServerErrorException" exception
    result = step.doStep(flightContext);
    verify(gcsPdao, times(2)).copyFile(any(), any(), any(), any());
    assertThat(
        "Step failed due to GoogleInternalServerErrorException and should be retried",
        StepStatus.STEP_RESULT_FAILURE_RETRY,
        equalTo(result.getStepStatus()));

    // 3 - Succeeds
    result = step.doStep(flightContext);
    verify(gcsPdao, times(3)).copyFile(any(), any(), any(), any());
    assertThat(
        "Retried step succeeds", StepStatus.STEP_RESULT_SUCCESS, equalTo(result.getStepStatus()));
  }

  @Test
  public void testDoStepExternallyHostedFailureThrows() {
    // Dataset is externally hosted by default
    String errorMessage = "failure";
    when(gcsPdao.copyFile(any(), any(), any(), any()))
        .thenThrow(new PdaoFileCopyException(errorMessage));

    PdaoFileCopyException thrown =
        assertThrows(
            PdaoFileCopyException.class,
            () -> step.doStep(flightContext),
            "Step throws unretryable exception");
    verify(gcsPdao, times(1)).copyFile(any(), any(), any(), any());
    assertThat("Error message reflects cause", thrown.getMessage(), equalTo(errorMessage));
  }

  @Test
  public void testThatFileIdIsProperlyCalculatedWhenPredictable() {
    // This is an ID that is a function of size, checksum and path of the file
    UUID predictableFileId = UUID.fromString("762d37d3-dccc-3e61-a0fd-c3768f3a975a");

    // Dataset is externally hosted by default
    FSFileInfo fileInfo = mock(FSFileInfo.class);
    when(fileInfo.getFileId()).thenReturn(predictableFileId.toString());
    when(fileInfo.getSize()).thenReturn(123L);
    when(fileInfo.getChecksumMd5()).thenReturn("foo");

    when(gcsPdao.copyFile(any(), any(), any(), any())).thenReturn(fileInfo);

    // Dataset uses predictable file ids
    when(dataset.hasPredictableFileIds()).thenReturn(true);

    step.doStep(flightContext);

    assertThat(
        "File ID is predicable",
        flightContext.getWorkingMap().get(FileMapKeys.FILE_ID, UUID.class),
        equalTo(predictableFileId));
  }

  @Test
  public void testThatFileIdIsProperlyCalculatedWhenRandom() {
    // Dataset is externally hosted by default
    FSFileInfo fileInfo = mock(FSFileInfo.class);
    when(fileInfo.getSize()).thenReturn(123L);
    when(fileInfo.getChecksumMd5()).thenReturn("foo");

    when(gcsPdao.copyFile(any(), any(), any(), any())).thenReturn(fileInfo);

    // Dataset uses predictable file ids
    when(dataset.hasPredictableFileIds()).thenReturn(false);

    step.doStep(flightContext);

    assertThat(
        "File ID is random",
        flightContext.getWorkingMap().get(FileMapKeys.FILE_ID, UUID.class),
        equalTo(RANDOM_FILE_ID));
  }
}

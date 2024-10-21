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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class IngestFilePrimaryDataStepTest {

  private static final UUID RANDOM_FILE_ID = UUID.randomUUID();

  private static final FileLoadModel FILE_LOAD_MODEL =
      new FileLoadModel()
          .loadTag("lt")
          .sourcePath("gs://bucket/path/file.txt")
          .targetPath("/foo/bar/baz.txt");

  @Mock private GcsPdao gcsPdao;

  @Mock private Dataset dataset;

  @Mock private ConfigurationService configService;

  private IngestFilePrimaryDataStep step;

  private FlightContext flightContext;

  @BeforeEach
  void setup() {
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
  }

  @Test
  void testDoStepSelfHostedRetryThenSucceed() {
    GoogleProjectResource projectResource =
        new GoogleProjectResource().googleProjectId("googleProjectId");
    when(dataset.getProjectResource()).thenReturn(projectResource);

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
    verify(gcsPdao).linkSelfHostedFile(any(), any(), any());
    assertThat(
        "Step failed due to InvalidUserProjectException and should be retried",
        result.getStepStatus(),
        equalTo(StepStatus.STEP_RESULT_FAILURE_RETRY));

    // 2 - throws retryable "GoogleInternalServerErrorException" exception
    result = step.doStep(flightContext);
    verify(gcsPdao, times(2)).linkSelfHostedFile(any(), any(), any());
    assertThat(
        "Step failed due to GoogleInternalServerErrorException and should be retried",
        result.getStepStatus(),
        equalTo(StepStatus.STEP_RESULT_FAILURE_RETRY));

    // 3 - Succeeds
    result = step.doStep(flightContext);
    verify(gcsPdao, times(3)).linkSelfHostedFile(any(), any(), any());
    assertThat(
        "Retried step succeeds", result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
  }

  @Test
  void testDoStepExternallyHostedRetryThenSucceed() {
    // Dataset is externally hosted by default
    FSFileInfo fileInfo = mock(FSFileInfo.class);
    // Step throws two retryable exceptions then succeeds
    when(gcsPdao.copyFile(any(), any(), any(), any()))
        .thenThrow(new InvalidUserProjectException("retryable"))
        .thenThrow(new GoogleInternalServerErrorException("retryable"))
        .thenReturn(fileInfo);

    // 1 - throws retryable "InvalidUserProjectException" exception
    StepResult result = step.doStep(flightContext);
    verify(gcsPdao).copyFile(any(), any(), any(), any());
    assertThat(
        "Step failed due to InvalidUserProjectException and should be retried",
        result.getStepStatus(),
        equalTo(StepStatus.STEP_RESULT_FAILURE_RETRY));

    // 2 - throws retryable "GoogleInternalServerErrorException" exception
    result = step.doStep(flightContext);
    verify(gcsPdao, times(2)).copyFile(any(), any(), any(), any());
    assertThat(
        "Step failed due to GoogleInternalServerErrorException and should be retried",
        result.getStepStatus(),
        equalTo(StepStatus.STEP_RESULT_FAILURE_RETRY));

    // 3 - Succeeds
    result = step.doStep(flightContext);
    verify(gcsPdao, times(3)).copyFile(any(), any(), any(), any());
    assertThat(
        "Retried step succeeds", result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
  }

  @Test
  void testDoStepExternallyHostedFailureThrows() {
    // Dataset is externally hosted by default
    String errorMessage = "failure";
    when(gcsPdao.copyFile(any(), any(), any(), any()))
        .thenThrow(new PdaoFileCopyException(errorMessage));

    PdaoFileCopyException thrown =
        assertThrows(
            PdaoFileCopyException.class,
            () -> step.doStep(flightContext),
            "Step throws unretryable exception");
    assertThat("Error message reflects cause", thrown.getMessage(), equalTo(errorMessage));
  }

  @Test
  void testThatFileIdIsProperlyCalculatedWhenPredictable() {
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
  void testThatFileIdIsProperlyCalculatedWhenRandom() {
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

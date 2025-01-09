package bio.terra.service.snapshot.flight.create;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

import bio.terra.common.FutureUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.ApiException;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.exception.FileSystemExecutionException;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepStatus;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class CreateSnapshotStorageTableDataStepTest {
  @Mock private TableDao tableDao;
  @Mock private AzureAuthService azureAuthService;
  @Mock private AzureSynapsePdao azureSynapsePdao;
  @Mock private SnapshotService snapshotService;
  @Mock private FlightContext flightContext;
  private FlightMap workingMap;
  private UUID datasetId;
  private String datasetName;
  private UUID snapshotId;
  private CreateSnapshotStorageTableDataStep step;

  @BeforeEach
  void setUp() {
    snapshotId = UUID.randomUUID();
    datasetId = UUID.randomUUID();
    datasetName = "datasetName";
    workingMap = new FlightMap();
    workingMap.put(
        CommonMapKeys.DATASET_STORAGE_AUTH_INFO,
        new AzureStorageAuthInfo(UUID.randomUUID(), "resourceGroup", "storageAccount"));
    workingMap.put(
        CommonMapKeys.SNAPSHOT_STORAGE_AUTH_INFO,
        new AzureStorageAuthInfo(UUID.randomUUID(), "resourceGroup", "storageAccount"));
    when(flightContext.getInputParameters()).thenReturn(new FlightMap());
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(azureAuthService.getTableServiceClient(any())).thenReturn(null);
    when(snapshotService.retrieve(any())).thenReturn(new Snapshot().id(snapshotId));

    when(azureSynapsePdao.getRefIdsForSnapshot(any()))
        .thenReturn(Set.of(UUID.randomUUID().toString()));
    step =
        new CreateSnapshotStorageTableDataStep(
            tableDao,
            azureAuthService,
            azureSynapsePdao,
            snapshotService,
            datasetId,
            datasetName,
            snapshotId);
  }

  @Test
  void retryInterruptedThread() throws InterruptedException {
    doThrow(new ApiException(FutureUtils.INTERRUPTED_THREAD_MESSAGE))
        .when(tableDao)
        .addFilesToSnapshot(any(), any(), any(), any(), any(), any());
    var result = step.doStep(flightContext);
    assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_RETRY));
  }

  @Test
  void fatalApiException() throws InterruptedException {
    doThrow(new ApiException("other error message"))
        .when(tableDao)
        .addFilesToSnapshot(any(), any(), any(), any(), any(), any());
    var result = step.doStep(flightContext);
    assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));
  }

  @Test
  void retryFileSystemExecutionException() throws InterruptedException {
    doThrow(new FileSystemExecutionException(""))
        .when(tableDao)
        .addFilesToSnapshot(any(), any(), any(), any(), any(), any());
    var result = step.doStep(flightContext);
    assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_RETRY));
  }

  @Test
  void addFilesToSnapshotSuccess() throws InterruptedException {
    doNothing().when(tableDao).addFilesToSnapshot(any(), any(), any(), any(), any(), any());
    var result = step.doStep(flightContext);
    assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
  }
}

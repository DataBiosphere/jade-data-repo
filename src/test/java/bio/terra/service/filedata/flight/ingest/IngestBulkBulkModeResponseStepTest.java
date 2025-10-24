package bio.terra.service.filedata.flight.ingest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.BulkLoadResultModel;
import bio.terra.service.dataset.flight.ingest.IngestMapKeys;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class IngestBulkBulkModeResponseStepTest {

  @Mock private FlightContext flightContext;

  @Mock private FlightMap workingMap;

  @Mock private BulkLoadResultModel loadSummary;

  private IngestBulkBulkModeResponseStep arrayModeStep;
  private IngestBulkBulkModeResponseStep nonArrayModeStep;

  @BeforeEach
  void setUp() {
    arrayModeStep = new IngestBulkBulkModeResponseStep(true);
    nonArrayModeStep = new IngestBulkBulkModeResponseStep(false);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  void doStep_arrayMode() {
    BulkLoadArrayResultModel result =
        new BulkLoadArrayResultModel().loadSummary(loadSummary).loadFileResults(List.of());
    when(workingMap.get(IngestMapKeys.BULK_LOAD_RESULT, BulkLoadArrayResultModel.class))
        .thenReturn(result);

    StepResult stepResult = arrayModeStep.doStep(flightContext);

    assertEquals(StepResult.getStepResultSuccess(), stepResult);
    verify(workingMap).put(JobMapKeys.RESPONSE.getKeyName(), result);
    verify(workingMap, never()).put(JobMapKeys.RESPONSE.getKeyName(), loadSummary);
  }

  @Test
  void doStep_nonArrayMode() {
    BulkLoadArrayResultModel result =
        new BulkLoadArrayResultModel().loadSummary(loadSummary).loadFileResults(List.of());
    when(workingMap.get(IngestMapKeys.BULK_LOAD_RESULT, BulkLoadArrayResultModel.class))
        .thenReturn(result);

    StepResult stepResult = nonArrayModeStep.doStep(flightContext);

    assertEquals(StepResult.getStepResultSuccess(), stepResult);
    verify(workingMap).put(JobMapKeys.RESPONSE.getKeyName(), loadSummary);
    verify(workingMap, never()).put(JobMapKeys.RESPONSE.getKeyName(), result);
  }

  @ParameterizedTest
  @ValueSource(ints = {500, 1000, 1500})
  void doStep_nonArrayMode_handlesFileResultsTruncation(int fileCount) {
    // Arrange
    List<BulkLoadFileResultModel> fileResults = createFileResults(fileCount);
    BulkLoadArrayResultModel result =
        new BulkLoadArrayResultModel().loadSummary(loadSummary).loadFileResults(fileResults);
    when(workingMap.get(IngestMapKeys.BULK_LOAD_RESULT, BulkLoadArrayResultModel.class))
        .thenReturn(result);

    // Act
    StepResult stepResult = nonArrayModeStep.doStep(flightContext);

    // Assert
    assertEquals(StepResult.getStepResultSuccess(), stepResult);
    int expectedSize = Math.min(fileCount, 1000);
    assertEquals(expectedSize, result.getLoadFileResults().size());
    verify(workingMap).put(JobMapKeys.RESPONSE.getKeyName(), loadSummary);
  }

  @Test
  void doStep_nonArrayMode_withNullFileResults_handlesGracefully() {
    BulkLoadArrayResultModel result =
        new BulkLoadArrayResultModel().loadSummary(loadSummary).loadFileResults(null);
    when(workingMap.get(IngestMapKeys.BULK_LOAD_RESULT, BulkLoadArrayResultModel.class))
        .thenReturn(result);

    StepResult stepResult = nonArrayModeStep.doStep(flightContext);

    assertEquals(StepResult.getStepResultSuccess(), stepResult);
    verify(workingMap).put(JobMapKeys.RESPONSE.getKeyName(), loadSummary);
  }

  @Test
  void doStep_arrayMode_withFileResultsOverLimit_doesNotTruncate() {
    List<BulkLoadFileResultModel> fileResults = createFileResults(1500);
    BulkLoadArrayResultModel result =
        new BulkLoadArrayResultModel().loadSummary(loadSummary).loadFileResults(fileResults);
    when(workingMap.get(IngestMapKeys.BULK_LOAD_RESULT, BulkLoadArrayResultModel.class))
        .thenReturn(result);

    StepResult stepResult = arrayModeStep.doStep(flightContext);

    assertEquals(StepResult.getStepResultSuccess(), stepResult);
    assertEquals(1500, result.getLoadFileResults().size());
    verify(workingMap).put(JobMapKeys.RESPONSE.getKeyName(), result);
  }

  private List<BulkLoadFileResultModel> createFileResults(int count) {
    return IntStream.range(0, count)
        .mapToObj(
            i ->
                new BulkLoadFileResultModel()
                    .targetPath("file" + i + ".txt")
                    .sourcePath("source/file" + i + ".txt")
                    .fileId("file-id-" + i))
        .collect(ArrayList::new, ArrayList::add, ArrayList::addAll);
  }
}

package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.service.dataset.flight.ingest.IngestMapKeys;
import bio.terra.service.dataset.flight.ingest.SkippableStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class IngestBuildLoadFileStep extends SkippableStep {
  private final ObjectMapper objectMapper;

  public IngestBuildLoadFileStep(
      ObjectMapper objectMapper, Predicate<FlightContext> skipCondition) {
    super(skipCondition);
    this.objectMapper = objectMapper;
  }

  public IngestBuildLoadFileStep(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public StepResult doSkippableStep(FlightContext context) {
    var workingMap = context.getWorkingMap();
    List<JsonNode> jsonLines = workingMap.get(IngestMapKeys.BULK_LOAD_JSON_LINES, List.class);
    List<String> fileColumns = workingMap.get(IngestMapKeys.TABLE_SCHEMA_FILE_COLUMNS, List.class);
    BulkLoadArrayResultModel result =
        workingMap.get(IngestMapKeys.BULK_LOAD_RESULT, BulkLoadArrayResultModel.class);

    // Part 1 -> Build the src-target hash to file id Map
    Map<Integer, String> pathToFileIdMap =
        result.getLoadFileResults().stream()
            .collect(
                Collectors.toMap(
                    model -> Objects.hash(model.getSourcePath(), model.getTargetPath()),
                    BulkLoadFileResultModel::getFileId));

    // Part 2 -> Replace BulkLoadFileModels with file id
    String linesWithFileIds =
        jsonLines.stream()
            .peek(
                node -> {
                  for (var columnName : fileColumns) {
                    JsonNode fileRefNode = node.get(columnName);
                    if (fileRefNode.isObject()) {
                      // replace
                      BulkLoadFileModel fileModel =
                          Optional.of(
                                  objectMapper.convertValue(fileRefNode, BulkLoadFileModel.class))
                              .orElseThrow();
                      int fileKey =
                          Objects.hash(fileModel.getSourcePath(), fileModel.getTargetPath());
                      String fileId = pathToFileIdMap.get(fileKey);
                      ((ObjectNode) node).put(columnName, fileId);
                    }
                  }
                })
            .map(JsonNode::toString)
            .collect(Collectors.joining("\n"));
    workingMap.put(IngestMapKeys.LINES_WITH_FILE_IDS, linesWithFileIds);

    return StepResult.getStepResultSuccess();
  }
}

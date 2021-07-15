package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.service.dataset.flight.ingest.IngestMapKeys;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class IngestBuildLoadFileStep implements Step {
  private final ObjectMapper objectMapper;

  public IngestBuildLoadFileStep(ObjectMapper objectMapper) {
    this.objectMapper = objectMapper;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    if (IngestUtils.noFilesToIngest(context)) {
      return StepResult.getStepResultSuccess();
    }

    var workingMap = context.getWorkingMap();
    List<JsonNode> jsonLines = workingMap.get(IngestMapKeys.BULK_LOAD_JSON_LINES, List.class);
    List<String> fileColumns = workingMap.get(IngestMapKeys.TABLE_SCHEMA_FILE_COLUMNS, List.class);
    BulkLoadArrayResultModel result =
        workingMap.get(IngestMapKeys.BULK_LOAD_RESULT, BulkLoadArrayResultModel.class);

    // Part 1 -> Build the src-target to file id Map
    Map<String, String> pathToFileIdMap =
        result.getLoadFileResults().stream()
            .collect(
                Collectors.toMap(
                    model -> model.getSourcePath() + "-" + model.getTargetPath(),
                    BulkLoadFileResultModel::getFileId));

    // Part 2 -> Replace BulkLoadFileModels with file id
    List<String> linesWithFileIds =
        jsonLines.stream()
            .peek(
                node -> {
                  for (var columnName : fileColumns) {
                    JsonNode fileRefNode = node.get(columnName);
                    if (fileRefNode.isObject()) {
                      // replace
                      BulkLoadFileModel fileModel =
                          Optional.of(
                                  objectMapper.convertValue(
                                      fileRefNode, bio.terra.model.BulkLoadFileModel.class))
                              .orElseThrow();
                      String fileKey = fileModel.getSourcePath() + "-" + fileModel.getTargetPath();
                      String fileId = pathToFileIdMap.get(fileKey);
                      ((ObjectNode) node).put(columnName, fileId);
                    }
                  }
                })
            .map(JsonNode::toString)
            .collect(Collectors.toList());
    workingMap.put(IngestMapKeys.LINES_WITH_FILE_IDS, linesWithFileIds);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}

package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.Column;
import bio.terra.common.ErrorCollector;
import bio.terra.common.FlightUtils;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.BulkLoadFileState;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.flight.ingest.IngestMapKeys;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class IngestBuildAndWriteScratchLoadFileStep extends DefaultUndoStep {
  protected final ObjectMapper objectMapper;
  protected final Dataset dataset;
  protected final int maxBadLoadFileLineErrorsReported;

  public IngestBuildAndWriteScratchLoadFileStep(
      ObjectMapper objectMapper, Dataset dataset, int maxBadLoadFileLineErrorsReported) {
    this.objectMapper = objectMapper;
    this.dataset = dataset;
    this.maxBadLoadFileLineErrorsReported = maxBadLoadFileLineErrorsReported;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    var workingMap = context.getWorkingMap();
    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);

    ErrorCollector errorCollector =
        new ErrorCollector(
            maxBadLoadFileLineErrorsReported,
            "Encountered invalid json while combining ingested files with load request");
    try (Stream<JsonNode> jsonNodes = getJsonNodesFromCloudFile(ingestRequest, errorCollector)) {

      List<Column> fileColumns = IngestUtils.getDatasetFileRefColumns(dataset, ingestRequest);
      BulkLoadArrayResultModel result =
          workingMap.get(IngestMapKeys.BULK_LOAD_RESULT, BulkLoadArrayResultModel.class);

      // Part 1 -> Build the src-target hash to file id Map
      Map<String, String> pathToFileIdMap =
          result.getLoadFileResults().stream()
              .filter(fileResultModel -> fileResultModel.getState() == BulkLoadFileState.SUCCEEDED)
              .collect(
                  Collectors.toMap(
                      BulkLoadFileResultModel::getTargetPath, BulkLoadFileResultModel::getFileId));

      // Part 1.1 -> Add in the already-ingested files, if told to do so
      if (ingestRequest.isResolveExistingFiles()) {
        Set<FileModel> existingFiles =
            FlightUtils.getTyped(workingMap, IngestMapKeys.COMBINED_EXISTING_FILES);
        existingFiles.forEach(
            fileModel -> pathToFileIdMap.put(fileModel.getPath(), fileModel.getFileId()));
      }

      AtomicLong failedRowCount = new AtomicLong();

      // Part 2 -> Replace BulkLoadFileModels with file id
      Stream<String> linesWithFileIds =
          jsonNodes
              .peek(node -> replaceNodeContents(node, fileColumns, pathToFileIdMap, failedRowCount))
              .filter(node -> !node.isEmpty()) // Filter out empty nodes.
              .map(JsonNode::toString);

      String path = getOutputFilePath(context);

      writeCloudFile(context, path, linesWithFileIds);
      // Check for parsing errors after new file is written because that's when
      // the stream is actually materialized.
      if (errorCollector.anyErrorsCollected()) {
        throw errorCollector.getFormattedException();
      }

      workingMap.put(IngestMapKeys.INGEST_CONTROL_FILE_PATH, path);
      workingMap.put(IngestMapKeys.COMBINED_FAILED_ROW_COUNT, failedRowCount.get());

      return StepResult.getStepResultSuccess();
    }
  }

  /**
   * Replace the fileRef members of the node with fileId if it's a BulkFileLoadModel, an array of
   * fileId if it's an array_of column, or do nothing to the node if it's already a fileId
   *
   * @param node The node in question.
   * @param fileColumns Columns in the node that correspond to fileRef of fileRef array_of columns
   * @param pathToFileIdMap A map from file target path to ingested fileId
   * @param failedRowCount A counter to count the failed ingests.
   */
  private void replaceNodeContents(
      JsonNode node,
      List<Column> fileColumns,
      Map<String, String> pathToFileIdMap,
      AtomicLong failedRowCount) {
    for (var column : fileColumns) {
      String columnName = column.getName();
      JsonNode fileRefNode = node.get(columnName);
      if (fileRefNode == null) {
        continue;
      }
      if (fileRefNode.isObject()) {
        replaceJsonObject(node, fileRefNode, columnName, pathToFileIdMap, failedRowCount);
      } else if (fileRefNode.isArray()) {
        replaceJsonArray(node, fileRefNode, columnName, pathToFileIdMap, failedRowCount);
      }
    }
  }

  /**
   * Replace a BulkFileLoadModel with a fileId in the node
   *
   * @param node The node in question
   * @param fileRefNode the BulkFileLoadModel node member
   * @param columnName The name of this fileRef column
   * @param pathToFileIdMap A map from file target path to ingested fileId
   * @param failedRowCount A counter to count the failed ingests.
   */
  private void replaceJsonObject(
      JsonNode node,
      JsonNode fileRefNode,
      String columnName,
      Map<String, String> pathToFileIdMap,
      AtomicLong failedRowCount) {
    // replace
    BulkLoadFileModel fileModel =
        Objects.requireNonNull(objectMapper.convertValue(fileRefNode, BulkLoadFileModel.class));
    String fileKey = fileModel.getTargetPath();
    if (pathToFileIdMap.containsKey(fileKey)) {
      String fileId = pathToFileIdMap.get(fileKey);
      ((ObjectNode) node).put(columnName, fileId);
    } else {
      // If the file wasn't ingested, clear the node.
      ((ObjectNode) node).removeAll();
      failedRowCount.getAndIncrement();
    }
  }

  /**
   * Replace an array_of column with BulkFileLoadModels with an array of fileIds in the node
   *
   * @param node The node in question
   * @param fileRefNode the array_of node member with a list of BulkFileLoadModels
   * @param columnName The name of this array_of fileRef column
   * @param pathToFileIdMap A map from file target path to ingested fileId
   * @param failedRowCount A counter to count the failed ingests.
   */
  private void replaceJsonArray(
      JsonNode node,
      JsonNode fileRefNode,
      String columnName,
      Map<String, String> pathToFileIdMap,
      AtomicLong failedRowCount) {

    // For consistency's sake, we should preserve the order of array_of values.
    // We do this by grabbing the values and putting them into 2 sparse arrays,
    // where a given index is populated in either one or the other arrays.
    // We can then reconstruct the fileRefNode value in-order once we grab all the
    // values from the map.

    int size = fileRefNode.size();
    BulkLoadFileModel[] models = new BulkLoadFileModel[size];
    String[] fileIds = new String[size];

    int i = 0;
    for (JsonNode fileRefArrayValue : fileRefNode) {
      if (fileRefArrayValue.isObject()) {
        models[i] = objectMapper.convertValue(fileRefArrayValue, BulkLoadFileModel.class);
      } else {
        fileIds[i] = fileRefArrayValue.asText();
      }
      i++;
    }

    boolean allIngested =
        Arrays.stream(models)
            .filter(Objects::nonNull)
            .allMatch(
                model -> {
                  String fileKey = model.getTargetPath();
                  return pathToFileIdMap.containsKey(fileKey);
                });
    if (allIngested) {
      ArrayNode value = objectMapper.createArrayNode();

      for (int j = 0; j < size; j++) {
        final String fileId;
        BulkLoadFileModel model = models[j];
        if (model != null) {
          fileId = pathToFileIdMap.get(model.getTargetPath());
        } else {
          fileId = fileIds[j];
        }
        value.add(fileId);
      }
      ((ObjectNode) node).replace(columnName, value);
    } else {
      ((ObjectNode) node).removeAll();
      failedRowCount.getAndIncrement();
    }
  }

  abstract Stream<JsonNode> getJsonNodesFromCloudFile(
      IngestRequestModel ingestRequest, ErrorCollector errorCollector);

  abstract String getOutputFilePath(FlightContext flightContext);

  abstract void writeCloudFile(FlightContext flightContext, String path, Stream<String> lines);
}

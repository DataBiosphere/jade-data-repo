package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.FlightUtils;
import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.BulkLoadFileState;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.flight.ingest.IngestMapKeys;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.dataset.flight.ingest.SkippableStep;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public abstract class IngestBuildAndWriteScratchLoadFileStep extends SkippableStep {
  protected final ObjectMapper objectMapper;

  public IngestBuildAndWriteScratchLoadFileStep(
      ObjectMapper objectMapper, Predicate<FlightContext> skipCondition) {
    super(skipCondition);
    this.objectMapper = objectMapper;
  }

  @Override
  public StepResult doSkippableStep(FlightContext context) {
    var workingMap = context.getWorkingMap();
    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);

    List<String> errors = new ArrayList<>();
    try (Stream<JsonNode> jsonNodes = getJsonNodesFromCloudFile(ingestRequest, errors)) {

      List<String> fileColumns =
          workingMap.get(IngestMapKeys.TABLE_SCHEMA_FILE_COLUMNS, List.class);
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
              .peek(
                  node -> {
                    for (var columnName : fileColumns) {
                      JsonNode fileRefNode = node.get(columnName);
                      if (fileRefNode != null && fileRefNode.isObject()) {
                        // replace
                        BulkLoadFileModel fileModel =
                            Objects.requireNonNull(
                                objectMapper.convertValue(fileRefNode, BulkLoadFileModel.class));
                        String fileKey = fileModel.getTargetPath();
                        if (pathToFileIdMap.containsKey(fileKey)) {
                          String fileId = pathToFileIdMap.get(fileKey);
                          ((ObjectNode) node).put(columnName, fileId);
                        } else {
                          // If the file wasn't ingest, clear the node.
                          ((ObjectNode) node).removeAll();
                          failedRowCount.getAndIncrement();
                        }
                      }
                    }
                  })
              .filter(node -> !node.isEmpty()) // Filter out empty nodes.
              .map(JsonNode::toString);

      String path = getOutputFilePath(context);

      writeCloudFile(context, path, linesWithFileIds);
      // Check for parsing errors after new file is written because that's when
      // the stream is actually materialized.
      if (!errors.isEmpty()) {
        throw new CorruptMetadataException(
            "Encountered invalid json while combining ingested files with load request");
      }

      workingMap.put(IngestMapKeys.INGEST_SCRATCH_FILE_PATH, path);
      workingMap.put(IngestMapKeys.COMBINED_FAILED_ROW_COUNT, failedRowCount.get());

      return StepResult.getStepResultSuccess();
    }
  }

  abstract Stream<JsonNode> getJsonNodesFromCloudFile(
      IngestRequestModel ingestRequest, List<String> errors);

  abstract String getOutputFilePath(FlightContext flightContext);

  abstract void writeCloudFile(FlightContext flightContext, String path, Stream<String> lines);
}

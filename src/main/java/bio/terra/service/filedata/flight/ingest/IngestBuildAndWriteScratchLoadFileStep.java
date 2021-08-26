package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.flight.ingest.IngestMapKeys;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.dataset.flight.ingest.SkippableStep;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
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
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IngestBuildAndWriteScratchLoadFileStep extends SkippableStep {
  private final ObjectMapper objectMapper;
  private final GcsPdao gcsPdao;
  private final Dataset dataset;

  public IngestBuildAndWriteScratchLoadFileStep(
      ObjectMapper objectMapper,
      GcsPdao gcsPdao,
      Dataset dataset,
      Predicate<FlightContext> skipCondition) {
    super(skipCondition);
    this.objectMapper = objectMapper;
    this.gcsPdao = gcsPdao;
    this.dataset = dataset;
  }

  @Override
  public StepResult doSkippableStep(FlightContext context) {
    var workingMap = context.getWorkingMap();
    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);

    List<String> errors = new ArrayList<>();
    Stream<JsonNode> jsonNodes =
        IngestUtils.getJsonNodesStreamFromFile(gcsPdao, objectMapper, ingestRequest, dataset)
            .flatMap(pair -> IngestUtils.resolveJsonNodeCollectError(pair, errors));

    if (!errors.isEmpty()) {
      // This shouldn't happen since this is the second time we're parsing the JSON
      throw new CorruptMetadataException(
          "Encountered invalid json while combining ingested files with load request");
    }

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
    Stream<String> linesWithFileIds =
        jsonNodes
            .peek(
                node -> {
                  for (var columnName : fileColumns) {
                    JsonNode fileRefNode = node.get(columnName);
                    if (Objects.nonNull(fileRefNode) && fileRefNode.isObject()) {
                      // replace
                      BulkLoadFileModel fileModel =
                          Objects.requireNonNull(
                              objectMapper.convertValue(fileRefNode, BulkLoadFileModel.class));
                      int fileKey =
                          Objects.hash(fileModel.getSourcePath(), fileModel.getTargetPath());
                      String fileId = pathToFileIdMap.get(fileKey);
                      ((ObjectNode) node).put(columnName, fileId);
                    }
                  }
                })
            .map(JsonNode::toString);

    GoogleBucketResource bucket =
        workingMap.get(FileMapKeys.INGEST_FILE_BUCKET_INFO, GoogleBucketResource.class);

    String path =
        GcsPdao.getGsPathFromComponents(bucket.getName(), context.getFlightId() + "-scratch.json");

    gcsPdao.createGcsFile(path, bucket.projectIdForBucket());
    gcsPdao.writeStreamToGcsFile(path, linesWithFileIds, bucket.projectIdForBucket());

    workingMap.put(IngestMapKeys.INGEST_SCRATCH_FILE_PATH, path);

    return StepResult.getStepResultSuccess();
  }
}

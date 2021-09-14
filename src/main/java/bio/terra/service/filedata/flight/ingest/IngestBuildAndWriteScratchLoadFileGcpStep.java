package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.BulkLoadArrayResultModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadFileResultModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.flight.ingest.IngestMapKeys;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
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

public class IngestBuildAndWriteScratchLoadFileGcpStep extends IngestBuildAndWriteScratchLoadFileStep {
  private final GcsPdao gcsPdao;
  private final Dataset dataset;

  public IngestBuildAndWriteScratchLoadFileGcpStep(
      ObjectMapper objectMapper,
      GcsPdao gcsPdao,
      Dataset dataset,
      Predicate<FlightContext> skipCondition) {
    super(objectMapper, skipCondition);
    this.gcsPdao = gcsPdao;
    this.dataset = dataset;
  }

  @Override
  Stream<JsonNode> getJsonNodesFromCloudFile(IngestRequestModel ingestRequest, List<String> errors) {
    return IngestUtils.getJsonNodesStreamFromFile(
        gcsPdao,
        objectMapper,
        ingestRequest,
        dataset.getProjectResource().getGoogleProjectId(),
        errors);
  }

  @Override
  String getOutputFilePath(FlightContext flightContext) {
    GoogleBucketResource bucket =
        flightContext.getWorkingMap().get(FileMapKeys.INGEST_FILE_BUCKET_INFO, GoogleBucketResource.class);

    return
        GcsUriUtils.getGsPathFromComponents(
            bucket.getName(), flightContext.getFlightId() + "-scratch.json");
  }

  @Override
  void writeCloudFile(FlightContext flightContext, String path, Stream<String> lines) {
    GoogleBucketResource bucket =
        flightContext.getWorkingMap().get(FileMapKeys.INGEST_FILE_BUCKET_INFO, GoogleBucketResource.class);
    gcsPdao.createGcsFile(path, bucket.projectIdForBucket());
    gcsPdao.writeStreamToCloudFile(path, lines, bucket.projectIdForBucket());
  }
}

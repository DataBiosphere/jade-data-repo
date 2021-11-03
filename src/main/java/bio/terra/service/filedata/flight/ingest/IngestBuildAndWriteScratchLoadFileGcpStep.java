package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.IngestRequestModel;
import bio.terra.service.common.gcs.CommonFlightKeys;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class IngestBuildAndWriteScratchLoadFileGcpStep
    extends IngestBuildAndWriteScratchLoadFileStep {
  private final GcsPdao gcsPdao;

  public IngestBuildAndWriteScratchLoadFileGcpStep(
      ObjectMapper objectMapper,
      GcsPdao gcsPdao,
      Dataset dataset,
      Predicate<FlightContext> doCondition) {
    super(objectMapper, dataset, doCondition);
    this.gcsPdao = gcsPdao;
  }

  @Override
  Stream<JsonNode> getJsonNodesFromCloudFile(
      IngestRequestModel ingestRequest, List<String> errors) {
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
        flightContext
            .getWorkingMap()
            .get(CommonFlightKeys.SCRATCH_BUCKET_INFO, GoogleBucketResource.class);

    return GcsUriUtils.getGsPathFromComponents(
        bucket.getName(), flightContext.getFlightId() + "/ingest-scratch.json");
  }

  @Override
  void writeCloudFile(FlightContext flightContext, String path, Stream<String> lines) {
    GoogleBucketResource bucket =
        flightContext
            .getWorkingMap()
            .get(CommonFlightKeys.SCRATCH_BUCKET_INFO, GoogleBucketResource.class);
    gcsPdao.createGcsFile(path, bucket.projectIdForBucket());
    gcsPdao.writeStreamToCloudFile(path, lines, bucket.projectIdForBucket());
  }

  @Override
  StepResult undo(FlightContext context) {
    IngestUtils.deleteScratchFile(context, gcsPdao);
    return StepResult.getStepResultSuccess();
  }
}

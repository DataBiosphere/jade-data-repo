package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadArrayRequestModel;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public class IngestBulkGcpArrayStep extends IngestBulkGcpStep {

  public IngestBulkGcpArrayStep(
      String loadTag,
      UUID profileId,
      AuthenticatedUserRequest userReq,
      GcsPdao gcsPdao,
      ObjectMapper objectMapper,
      Dataset dataset,
      int maxFailedFileLoads,
      int maxBadLoadFileLineErrorsReported,
      FireStoreDao fileDao,
      FileService fileService,
      ExecutorService executor,
      int maxPerformanceThreadQueueSize) {
    super(
        loadTag,
        profileId,
        userReq,
        gcsPdao,
        objectMapper,
        dataset,
        maxFailedFileLoads,
        maxBadLoadFileLineErrorsReported,
        fileDao,
        fileService,
        executor,
        maxPerformanceThreadQueueSize);
  }

  @Override
  public Stream<BulkLoadFileModel> getModelsStream(FlightContext context) {
    BulkLoadArrayRequestModel ingestRequest =
        context
            .getInputParameters()
            .get(JobMapKeys.REQUEST.getKeyName(), BulkLoadArrayRequestModel.class);

    return ingestRequest.getLoadArray().stream();
  }
}

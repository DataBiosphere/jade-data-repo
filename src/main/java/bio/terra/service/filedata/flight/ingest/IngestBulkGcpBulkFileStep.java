package bio.terra.service.filedata.flight.ingest;

import bio.terra.common.ErrorCollector;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.BulkLoadRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public class IngestBulkGcpBulkFileStep extends IngestBulkGcpStep {

  public IngestBulkGcpBulkFileStep(
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
    BulkLoadRequestModel ingestRequest =
        context
            .getInputParameters()
            .get(JobMapKeys.REQUEST.getKeyName(), BulkLoadRequestModel.class);
    ErrorCollector errorCollector =
        new ErrorCollector(
            maxBadLoadFileLineErrorsReported,
            "Ingest control file at "
                + ingestRequest.getLoadControlFile()
                + " could not be processed");

    var cloudEncapsulationId = dataset.getProjectResource().getGoogleProjectId();
    try (var nodesStream =
        IngestUtils.getStreamFromFile(
            gcsPdao,
            objectMapper,
            ingestRequest.getLoadControlFile(),
            userReq,
            cloudEncapsulationId,
            errorCollector,
            // Casting explicitly since it's losing the type downstream
            new TypeReference<BulkLoadFileModel>() {})) {
      return IngestUtils.validateBulkFileLoadModelsFromStream(
          nodesStream, gcsPdao, cloudEncapsulationId, userReq, errorCollector, dataset);
    }
  }
}

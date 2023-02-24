package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.common.ErrorCollector;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.flight.ingest.IngestBulkGcpStep;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.stairway.FlightContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

public class IngestBulkGcpCombinedIngestStep extends IngestBulkGcpStep {

  public IngestBulkGcpCombinedIngestStep(
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
    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);
    List<Column> fileRefColumns = IngestUtils.getDatasetFileRefColumns(dataset, ingestRequest);
    ErrorCollector errorCollector =
        new ErrorCollector(
            maxBadLoadFileLineErrorsReported,
            "Ingest control file at " + ingestRequest.getPath() + " could not be processed");

    return IngestUtils.getBulkFileLoadModelsStream(
        gcsPdao,
        objectMapper,
        ingestRequest,
        userReq,
        dataset.getProjectResource().getGoogleProjectId(),
        fileRefColumns,
        errorCollector);
  }
}

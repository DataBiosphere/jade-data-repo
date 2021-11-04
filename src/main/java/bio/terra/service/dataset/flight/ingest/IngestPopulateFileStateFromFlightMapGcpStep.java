package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FileService;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.load.LoadService;
import bio.terra.stairway.FlightContext;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Stream;

public class IngestPopulateFileStateFromFlightMapGcpStep
    extends IngestPopulateFileStateFromFlightMapStep {

  private final GcsPdao gcsPdao;
  private final AuthenticatedUserRequest userRequest;

  public IngestPopulateFileStateFromFlightMapGcpStep(
      LoadService loadService,
      FileService fileService,
      GcsPdao gcsPdao,
      ObjectMapper objectMapper,
      Dataset dataset,
      int batchSize,
      AuthenticatedUserRequest userRequest,
      Predicate<FlightContext> doCondition) {
    super(loadService, fileService, objectMapper, dataset, batchSize, doCondition);
    this.gcsPdao = gcsPdao;
    this.userRequest = userRequest;
  }

  @Override
  Stream<BulkLoadFileModel> getModelsStream(
      IngestRequestModel ingestRequest, List<Column> fileRefColumns, List<String> errors) {
    return IngestUtils.getBulkFileLoadModelsStream(
        gcsPdao,
        objectMapper,
        ingestRequest,
        userRequest,
        dataset.getProjectResource().getGoogleProjectId(),
        fileRefColumns,
        errors);
  }
}

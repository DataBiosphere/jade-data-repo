package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.common.ErrorCollector;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;

public class IngestJsonFileSetupGcpStep extends IngestJsonFileSetupStep {

  private final GcsPdao gcsPdao;
  private final ObjectMapper objectMapper;
  private final AuthenticatedUserRequest userRequest;

  public IngestJsonFileSetupGcpStep(
      GcsPdao gcsPdao,
      ObjectMapper objectMapper,
      Dataset dataset,
      AuthenticatedUserRequest userRequest,
      int maxBadLoadFileLineErrorsReported) {
    super(dataset, maxBadLoadFileLineErrorsReported);
    this.gcsPdao = gcsPdao;
    this.objectMapper = objectMapper;
    this.userRequest = userRequest;
  }

  @Override
  long getFileModelsCount(
      IngestRequestModel ingestRequest,
      List<Column> fileRefColumns,
      ErrorCollector errorCollector) {
    return IngestUtils.countAndValidateBulkFileLoadModelsFromPath(
        gcsPdao,
        objectMapper,
        ingestRequest,
        userRequest,
        dataset.getProjectResource().getGoogleProjectId(),
        fileRefColumns,
        errorCollector,
        dataset);
  }
}

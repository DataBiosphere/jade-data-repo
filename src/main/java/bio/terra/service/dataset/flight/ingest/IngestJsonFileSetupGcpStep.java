package bio.terra.service.dataset.flight.ingest;

import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.stairway.FlightMap;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;

public class IngestJsonFileSetupGcpStep extends IngestJsonFileSetupStep {

  private final GcsPdao gcsPdao;
  private final ObjectMapper objectMapper;

  public IngestJsonFileSetupGcpStep(GcsPdao gcsPdao, ObjectMapper objectMapper, Dataset dataset) {
    super(dataset);
    this.gcsPdao = gcsPdao;
    this.objectMapper = objectMapper;
  }

  @Override
  long getFileModelsCount(
      IngestRequestModel ingestRequest,
      FlightMap workingMap,
      List<String> fileRefColumnNames,
      List<String> errors) {
    return IngestUtils.countBulkFileLoadModelsFromPath(
        gcsPdao,
        objectMapper,
        ingestRequest,
        dataset.getProjectResource().getGoogleProjectId(),
        fileRefColumnNames,
        errors);
  }
}

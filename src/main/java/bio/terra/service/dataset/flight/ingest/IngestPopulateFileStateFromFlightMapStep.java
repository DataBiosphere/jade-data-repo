package bio.terra.service.dataset.flight.ingest;

import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.exception.IngestFailureException;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.service.snapshot.exception.CorruptMetadataException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.function.Predicate;

public class IngestPopulateFileStateFromFlightMapStep extends SkippableStep {

  private final LoadService loadService;
  private final GcsPdao gcsPdao;
  private final ObjectMapper objectMapper;
  private final Dataset dataset;

  public IngestPopulateFileStateFromFlightMapStep(
      LoadService loadService,
      GcsPdao gcsPdao,
      ObjectMapper objectMapper,
      Dataset dataset,
      Predicate<FlightContext> skipCondition) {
    super(skipCondition);
    this.loadService = loadService;
    this.gcsPdao = gcsPdao;
    this.objectMapper = objectMapper;
    this.dataset = dataset;
  }

  @Override
  public StepResult doSkippableStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    UUID loadId = UUID.fromString(workingMap.get(LoadMapKeys.LOAD_ID, String.class));
    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);
    List<String> fileColumns = workingMap.get(IngestMapKeys.TABLE_SCHEMA_FILE_COLUMNS, List.class);

    List<String> errors = new ArrayList<>();
    Set<BulkLoadFileModel> models =
        IngestUtils.getBulkFileLoadModelsFromPath(
            gcsPdao, objectMapper, ingestRequest, dataset, fileColumns, errors);

    if (!errors.isEmpty()) {
      IngestFailureException ingestFailureException =
          new IngestFailureException(
              "Ingest control file at " + ingestRequest.getPath() + " could not be processed",
              errors);
      CorruptMetadataException ex =
          new CorruptMetadataException(
              "Ingest control file went from valid to invalid within combined ingest.",
              ingestFailureException);
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    loadService.populateFiles(loadId, List.copyOf(models));

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoSkippableStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    UUID loadId = UUID.fromString(workingMap.get(LoadMapKeys.LOAD_ID, String.class));

    loadService.cleanFiles(loadId);
    return StepResult.getStepResultSuccess();
  }
}

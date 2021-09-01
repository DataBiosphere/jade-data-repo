package bio.terra.service.dataset.flight.ingest;

import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.exception.IngestFailureException;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Predicate;

public class IngestPopulateFileStateFromFlightMapStep extends SkippableStep {

  private final LoadService loadService;
  private final GcsPdao gcsPdao;
  private final ObjectMapper objectMapper;
  private final Dataset dataset;
  private final int batchSize;

  public IngestPopulateFileStateFromFlightMapStep(
      LoadService loadService,
      GcsPdao gcsPdao,
      ObjectMapper objectMapper,
      Dataset dataset,
      int batchSize,
      Predicate<FlightContext> skipCondition) {
    super(skipCondition);
    this.loadService = loadService;
    this.gcsPdao = gcsPdao;
    this.objectMapper = objectMapper;
    this.dataset = dataset;
    this.batchSize = batchSize;
  }

  // For some reason, Spotbugs thinks the try-with-resources results in a redundant nullcheck...
  // This appears to be a bug in Spotbugs. https://github.com/spotbugs/spotbugs/issues/756
  @Override
  @SuppressFBWarnings("RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE")
  public StepResult doSkippableStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    UUID loadId = UUID.fromString(workingMap.get(LoadMapKeys.LOAD_ID, String.class));
    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);
    List<String> fileColumns = workingMap.get(IngestMapKeys.TABLE_SCHEMA_FILE_COLUMNS, List.class);

    List<String> errors = new ArrayList<>();
    try (var bulkFileLoadModels =
        IngestUtils.getBulkFileLoadModelsStream(
            gcsPdao, objectMapper, ingestRequest, dataset, fileColumns, errors)) {

      loadService.populateFiles(loadId, bulkFileLoadModels, batchSize);

      // Check for parsing errors after files are populated in the load table because that's when
      // the stream is actually materialized.
      if (!errors.isEmpty()) {
        IngestFailureException ingestFailureException =
            new IngestFailureException(
                "Ingest control file at " + ingestRequest.getPath() + " could not be processed",
                errors);
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ingestFailureException);
      }
    }
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

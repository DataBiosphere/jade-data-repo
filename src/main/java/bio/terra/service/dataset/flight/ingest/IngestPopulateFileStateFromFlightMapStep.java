package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.common.ErrorCollector;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.FileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FileService;
import bio.terra.service.load.LoadService;
import bio.terra.service.load.flight.LoadMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Stream;
import org.apache.commons.lang3.StringUtils;

public abstract class IngestPopulateFileStateFromFlightMapStep implements Step {

  private final LoadService loadService;
  private final FileService fileService;
  final ObjectMapper objectMapper;
  final Dataset dataset;
  private final int batchSize;
  private final int maxBadLoadFileLineErrorsReported;

  public IngestPopulateFileStateFromFlightMapStep(
      LoadService loadService,
      FileService fileService,
      ObjectMapper objectMapper,
      Dataset dataset,
      int batchSize,
      int maxBadLoadFileLineErrorsReported) {
    this.loadService = loadService;
    this.fileService = fileService;
    this.objectMapper = objectMapper;
    this.dataset = dataset;
    this.batchSize = batchSize;
    this.maxBadLoadFileLineErrorsReported = maxBadLoadFileLineErrorsReported;
  }

  @Override
  @SuppressFBWarnings(
      value = "RCN_REDUNDANT_NULLCHECK_WOULD_HAVE_BEEN_A_NPE",
      justification = "Spotbugs doesn't understand resource try construct")
  public StepResult doStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    UUID loadId = UUID.fromString(workingMap.get(LoadMapKeys.LOAD_ID, String.class));
    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(context);
    List<Column> fileColumns = IngestUtils.getDatasetFileRefColumns(dataset, ingestRequest);

    ErrorCollector errorCollector =
        new ErrorCollector(
            maxBadLoadFileLineErrorsReported,
            "Ingest control file at " + ingestRequest.getPath() + " could not be processed");
    try (var bulkFileLoadModels = getModelsStream(ingestRequest, fileColumns, errorCollector)) {

      if (ingestRequest.isResolveExistingFiles()) {
        Set<FileModel> existingFiles = new HashSet<>();
        var toIngest =
            bulkFileLoadModels
                .map(fileModel -> fileToIngest(fileModel, existingFiles))
                .filter(Objects::nonNull);
        loadService.populateFiles(loadId, toIngest, batchSize);
        workingMap.put(IngestMapKeys.COMBINED_EXISTING_FILES, existingFiles);
      } else {
        loadService.populateFiles(loadId, bulkFileLoadModels, batchSize);
      }

      // Check for parsing errors after files are populated in the load table because that's when
      // the stream is actually materialized.
      if (errorCollector.anyErrorsCollected()) {
        return new StepResult(
            StepStatus.STEP_RESULT_FAILURE_FATAL, errorCollector.getFormattedException());
      }
    }
    return StepResult.getStepResultSuccess();
  }

  /**
   * Filter the stream so that only files that need to be ingested are ingested, but also save the
   * files that have already been ingested.
   *
   * @param bulkLoadModel A candidate file to maybe ingest
   * @param collector A place to put the FileModels that already exist
   * @return A Stream of BulkLoadFileModel if it needs to be ingested, or an empty Stream if not.
   */
  private BulkLoadFileModel fileToIngest(
      BulkLoadFileModel bulkLoadModel, Set<FileModel> collector) {
    int depth = StringUtils.countMatches(bulkLoadModel.getTargetPath(), "/");
    Optional<FileModel> file =
        fileService.lookupOptionalPath(
            dataset.getId().toString(), bulkLoadModel.getTargetPath(), depth);
    if (file.isPresent()) {
      // File exists. Don't ingest it, but add it to the collector.
      collector.add(file.get());
      return null;
    } else {
      // File doesn't exist. Ingest it!
      return bulkLoadModel;
    }
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    FlightMap workingMap = context.getWorkingMap();
    UUID loadId = UUID.fromString(workingMap.get(LoadMapKeys.LOAD_ID, String.class));

    loadService.cleanFiles(loadId);
    return StepResult.getStepResultSuccess();
  }

  abstract Stream<BulkLoadFileModel> getModelsStream(
      IngestRequestModel ingestRequest, List<Column> fileRefColumns, ErrorCollector errorCollector);
}

package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.TableDataType;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.exception.IngestFailureException;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class IngestJsonFileSetupStep implements Step {

  private final GcsPdao gcsPdao;
  private final Dataset dataset;
  private final ObjectMapper objectMapper;
  private final ConfigurationService configurationService;

  public IngestJsonFileSetupStep(
      GcsPdao gcsPdao,
      ObjectMapper objectMapper,
      Dataset dataset,
      ConfigurationService configurationService) {
    this.gcsPdao = gcsPdao;
    this.objectMapper = objectMapper;
    this.dataset = dataset;
    this.configurationService = configurationService;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(flightContext);
    List<String> fileRefColumnNames =
        dataset.getTableByName(ingestRequest.getTable()).orElseThrow().getColumns().stream()
            .filter(c -> c.getType() == TableDataType.FILEREF)
            .map(Column::getName)
            .collect(Collectors.toList());

    var workingMap = flightContext.getWorkingMap();

    // If there's no FILEREF columns, we never need to parse the ingest control file.
    if (fileRefColumnNames.isEmpty()) {
      // Defaults so that other steps don't NPE
      workingMap.put(IngestMapKeys.NUM_BULK_LOAD_FILE_MODELS, 0);
      workingMap.put(IngestMapKeys.TABLE_SCHEMA_FILE_COLUMNS, Collections.emptyList());
      return StepResult.getStepResultSuccess();
    }

    workingMap.put(IngestMapKeys.TABLE_SCHEMA_FILE_COLUMNS, fileRefColumnNames);

    List<String> errors = new ArrayList<>();
    // Parse the file models, but don't save them because we don't want to blow up the database.
    // We read from the ingest control file each time we need to get the models to ingest.
    long fileModelsCount =
        IngestUtils.countBulkFileLoadModelsFromPath(
            gcsPdao, objectMapper, ingestRequest, dataset, fileRefColumnNames, errors);

    if (!errors.isEmpty()) {
      IngestFailureException ex =
          new IngestFailureException(
              "Ingest control file at " + ingestRequest.getPath() + " could not be processed",
              errors);
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    workingMap.put(IngestMapKeys.NUM_BULK_LOAD_FILE_MODELS, fileModelsCount);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}

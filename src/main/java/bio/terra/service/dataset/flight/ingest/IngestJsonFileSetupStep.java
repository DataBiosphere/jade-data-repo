package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.model.IngestRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.exception.IngestFailureException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import java.util.ArrayList;
import java.util.List;

public abstract class IngestJsonFileSetupStep implements Step {

  final Dataset dataset;

  public IngestJsonFileSetupStep(Dataset dataset) {
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(flightContext);
    List<Column> fileRefColumns = IngestUtils.getDatasetFileRefColumns(dataset, ingestRequest);

    var workingMap = flightContext.getWorkingMap();

    // If there's no FILEREF columns, we never need to parse the ingest-control file.
    if (fileRefColumns.isEmpty()) {
      // Defaults so that other steps don't NPE
      workingMap.put(IngestMapKeys.NUM_BULK_LOAD_FILE_MODELS, 0);
      return StepResult.getStepResultSuccess();
    }

    List<String> errors = new ArrayList<>();
    // Parse the file models, but don't save them because we don't want to blow up the database.
    // We read from the ingest control file each time we need to get the models to ingest.
    long fileModelsCount = getFileModelsCount(ingestRequest, fileRefColumns, errors);

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

  /**
   * Count the file models in the ingest-control file
   *
   * @param ingestRequest IngestRequestModel with path to control file
   * @param fileRefColumnNames Column names that are of type FILEREF
   * @param errors List to accumulate errors in parsing
   * @return The number of file ingests that would need to be performed for this control file
   */
  abstract long getFileModelsCount(
      IngestRequestModel ingestRequest, List<Column> fileRefColumnNames, List<String> errors);
}

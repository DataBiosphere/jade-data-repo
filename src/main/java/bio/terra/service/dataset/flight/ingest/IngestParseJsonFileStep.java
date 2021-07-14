package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.common.JsonParseUtils;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class IngestParseJsonFileStep implements Step {

  private final GcsPdao gcsPdao;
  private final Dataset dataset;
  private final ObjectMapper objectMapper;

  public IngestParseJsonFileStep(GcsPdao gcsPdao, ObjectMapper objectMapper, Dataset dataset) {
    this.gcsPdao = gcsPdao;
    this.objectMapper = objectMapper;
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext flightContext)
      throws InterruptedException, RetryException {
    var workingMap = flightContext.getWorkingMap();
    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(flightContext);

    // Read ingest request from gcs into list of strings; each line representing json objects
    List<String> gcsFileLines = gcsPdao.getGcsFileLines(ingestRequest.getPath());

    // Columns in Table w/ type Fileref
    List<String> fileRefColumnNames = dataset.getTableColumnsOfType(ingestRequest.getTable(),
        TableDataType.FILEREF);
    workingMap.put(IngestMapKeys.TABLE_SCHEMA_FILE_COLUMNS, fileRefColumnNames);

    try {
      // Parse lines from ingest request file into json
      List<JsonNode> fileLineJson = IngestUtils.parse(gcsFileLines, objectMapper);
      workingMap.put(IngestMapKeys.BULK_LOAD_JSON_LINES, fileLineJson);

      // Collect list of files that need to be ingested
      Set<BulkLoadFileModel> bulkLoadFileModels = IngestUtils.collectFilesForIngest(fileLineJson,
          fileRefColumnNames,
          objectMapper);

      workingMap.put(IngestMapKeys.BULK_LOAD_FILE_MODELS, bulkLoadFileModels);

      return StepResult.getStepResultSuccess();

    } catch (Exception ex) {
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}

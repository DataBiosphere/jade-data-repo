package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.exception.IngestFailureException;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
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
    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(flightContext);
    // TODO: Is this ok? Can we charge the dataset's project?
    List<String> gcsFileLines =
        gcsPdao.getGcsFileLines(
            ingestRequest.getPath(), dataset.getProjectResource().getGoogleProjectId());
    List<String> fileRefColumnNames =
        dataset.getTableByName(ingestRequest.getTable()).orElseThrow().getColumns().stream()
            .filter(c -> c.getType() == TableDataType.FILEREF)
            .map(Column::getName)
            .collect(Collectors.toList());
    var workingMap = flightContext.getWorkingMap();
    workingMap.put(IngestMapKeys.TABLE_SCHEMA_FILE_COLUMNS, fileRefColumnNames);

    List<String> errors = new ArrayList<>();
    List<JsonNode> fileLineJson =
        gcsFileLines.stream()
            .map(
                content -> {
                  try {
                    return objectMapper.readTree(content);
                  } catch (JsonProcessingException ex) {
                    errors.add(ex.getMessage());
                    return null;
                  }
                })
            .collect(Collectors.toList());

    if (!errors.isEmpty()) {
      IngestFailureException ex =
          new IngestFailureException(
              "Ingest control file at " + ingestRequest.getPath() + " could not be processed",
              errors);
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    workingMap.put(IngestMapKeys.BULK_LOAD_JSON_LINES, fileLineJson);
    Set<BulkLoadFileModel> bulkLoadFileModels =
        fileLineJson.stream()
            .flatMap(
                node ->
                    fileRefColumnNames.stream()
                        .map(
                            columnName -> {
                              JsonNode fileRefNode = node.get(columnName);
                              if (fileRefNode.isObject()) {
                                return Optional.of(
                                    objectMapper.convertValue(
                                        fileRefNode, BulkLoadFileModel.class));
                              } else {
                                return Optional.<BulkLoadFileModel>empty();
                              }
                            })
                        .filter(Optional::isPresent)
                        .map(Optional::get))
            .collect(Collectors.toSet());

    workingMap.put(IngestMapKeys.BULK_LOAD_FILE_MODELS, bulkLoadFileModels);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}

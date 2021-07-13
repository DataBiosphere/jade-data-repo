package bio.terra.service.dataset.flight.ingest;

import bio.terra.common.Column;
import bio.terra.model.BulkLoadFileModel;
import bio.terra.model.IngestRequestModel;
import bio.terra.model.TableDataType;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import bio.terra.stairway.exception.RetryException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

public class IngestParseJsonFileStep implements Step {

  private final GcsPdao gcsPdao;
  private final DatasetService datasetService;
  private final ObjectMapper objectMapper;

  public IngestParseJsonFileStep(GcsPdao gcsPdao, DatasetService datasetService, ObjectMapper objectMapper) {
    this.gcsPdao = gcsPdao;
    this.datasetService = datasetService;
    this.objectMapper = objectMapper;
  }

  @Override
  public StepResult doStep(FlightContext flightContext) throws InterruptedException, RetryException {
    IngestRequestModel ingestRequest = IngestUtils.getIngestRequestModel(flightContext);
    List<String> gcsFileLines = gcsPdao.getGcsFileLines(ingestRequest.getPath());
    Dataset dataset = IngestUtils.getDataset(flightContext, datasetService);
    List<String> fileRefColumnNames = dataset.getTableByName(ingestRequest.getTable())
        .orElseThrow()
        .getColumns()
        .stream().filter(c -> c.getType() == TableDataType.FILEREF)
        .map(Column::getName)
        .collect(Collectors.toList());
    try {
      Set<BulkLoadFileModel> bulkLoadFileModels = gcsFileLines.stream()
          .map(content -> {
            try {
              return objectMapper.readTree(content);
            } catch (JsonProcessingException e) {
              throw new RuntimeException(e);
            }
          })
          .flatMap(node -> fileRefColumnNames.stream()
              .map(columnName -> {
                JsonNode fileRefNode = node.get(columnName);
                if (fileRefNode.isObject()) {
                  return Optional.of(objectMapper.convertValue(fileRefNode, BulkLoadFileModel.class));
                } else {
                  return Optional.<BulkLoadFileModel>empty();
                }
              }).filter(Optional::isPresent)
              .map(Optional::get))
          .collect(Collectors.toSet());

      var workingMap = flightContext.getWorkingMap();
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

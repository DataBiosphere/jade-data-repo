package bio.terra.service.dataset.flight.ingest;

import bio.terra.app.configuration.ApplicationConfiguration;
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
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class IngestParseJsonFileStep implements Step {

  private final GcsPdao gcsPdao;
  private final Dataset dataset;
  private final ObjectMapper objectMapper;
  private final ApplicationConfiguration applicationConfiguration;

  public IngestParseJsonFileStep(
      GcsPdao gcsPdao,
      ObjectMapper objectMapper,
      Dataset dataset,
      ApplicationConfiguration applicationConfiguration) {
    this.gcsPdao = gcsPdao;
    this.objectMapper = objectMapper;
    this.dataset = dataset;
    this.applicationConfiguration = applicationConfiguration;
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
    workingMap.put(IngestMapKeys.TABLE_SCHEMA_FILE_COLUMNS, fileRefColumnNames);

    List<String> errors = new ArrayList<>();
    Set<BulkLoadFileModel> fileModels =
        IngestUtils.getJsonNodesStreamFromFile(gcsPdao, objectMapper, ingestRequest, dataset)
            .flatMap(pair -> IngestUtils.resolveJsonNodeCollectError(pair, errors))
            .flatMap(
                node ->
                    fileRefColumnNames.stream()
                        .flatMap(
                            columnName -> {
                              JsonNode fileRefNode = node.get(columnName);
                              if (fileRefNode.isObject()) {
                                return Stream.of(
                                    objectMapper.convertValue(
                                        fileRefNode, BulkLoadFileModel.class));
                              } else {
                                return Stream.empty();
                              }
                            }))
            .collect(Collectors.toSet());

    if (!errors.isEmpty()) {
      IngestFailureException ex =
          new IngestFailureException(
              "Ingest control file at " + ingestRequest.getPath() + " could not be processed",
              errors);
      return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
    }

    workingMap.put(IngestMapKeys.BULK_LOAD_FILE_MODELS, fileModels);

    IngestUtils.checkForLargeIngestRequests(
        fileModels.size(), applicationConfiguration.getMaxCombinedFileAndMetadataIngest());

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext flightContext) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}

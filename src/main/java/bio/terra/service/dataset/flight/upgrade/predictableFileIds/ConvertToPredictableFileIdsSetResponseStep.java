package bio.terra.service.dataset.flight.upgrade.predictableFileIds;

import bio.terra.common.Column;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.Map;
import java.util.UUID;

public record ConvertToPredictableFileIdsSetResponseStep(
    UUID datasetId, DatasetService datasetService) implements DefaultUndoStep {
  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Dataset dataset = datasetService.retrieve(datasetId);
    Map<UUID, UUID> oldToNewMappings =
        ConvertFileIdUtils.readFlightMappings(context.getWorkingMap());
    long numTables = dataset.getTables().size();
    long numTablesConverted =
        dataset.getTables().stream()
            .filter(t -> t.getColumns().stream().anyMatch(Column::isFileOrDirRef))
            .count();
    String responseMessage =
        "Migrated dataset %s: %s file Ids updated across %s of %s tables"
            .formatted(
                dataset.toLogString(),
                oldToNewMappings.keySet().size(),
                numTablesConverted,
                numTables);
    context.getWorkingMap().put(JobMapKeys.RESPONSE.getKeyName(), responseMessage);
    return StepResult.getStepResultSuccess();
  }
}

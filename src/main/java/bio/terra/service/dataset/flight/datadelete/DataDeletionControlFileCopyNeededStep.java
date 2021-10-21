package bio.terra.service.dataset.flight.datadelete;

import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getDataset;
import static bio.terra.service.dataset.flight.datadelete.DataDeletionUtils.getRequest;

import bio.terra.app.model.CloudRegion;
import bio.terra.app.model.GoogleCloudResource;
import bio.terra.model.DataDeletionRequest;
import bio.terra.model.DataDeletionTableModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class DataDeletionControlFileCopyNeededStep implements Step {

  private final DatasetService datasetService;
  private final GcsPdao gcsPdao;

  public DataDeletionControlFileCopyNeededStep(DatasetService datasetService, GcsPdao gcsPdao) {
    this.datasetService = datasetService;
    this.gcsPdao = gcsPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    Dataset dataset = getDataset(context, datasetService);
    DataDeletionRequest dataDeletionRequest = getRequest(context);
    CloudRegion bigQueryRegion =
        dataset.getDatasetSummary().getStorageResourceRegion(GoogleCloudResource.BIGQUERY);

    String projectId = dataset.getProjectResource().getGoogleProjectId();
    Predicate<DataDeletionTableModel> fileNotInRegion =
        fileNotInRegionPredicate(bigQueryRegion, projectId);
    Set<String> tableNamesNeedingCopy =
        dataDeletionRequest.getTables().stream()
            .filter(fileNotInRegion)
            .map(DataDeletionTableModel::getTableName)
            .collect(Collectors.toSet());

    workingMap.put(DataDeletionMapKeys.TABLE_NAMES_NEEDING_COPY, tableNamesNeedingCopy);

    if (tableNamesNeedingCopy.isEmpty()) {
      workingMap.put(DataDeletionMapKeys.TABLES, dataDeletionRequest.getTables());
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return null;
  }

  Predicate<DataDeletionTableModel> fileNotInRegionPredicate(
      CloudRegion bigQueryRegion, String projectId) {
    return (model) -> {
      String path = model.getGcsFileSpec().getPath();
      return gcsPdao.getRegionForFile(path, projectId) != bigQueryRegion;
    };
  }
}

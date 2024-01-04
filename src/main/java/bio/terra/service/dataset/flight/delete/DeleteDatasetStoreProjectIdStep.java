package bio.terra.service.dataset.flight.delete;

import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class DeleteDatasetStoreProjectIdStep implements Step {
  private final UUID datasetId;
  private final DatasetService datasetService;
  private final DatasetBucketDao datasetBucketDao;

  public DeleteDatasetStoreProjectIdStep(
      UUID datasetId, DatasetService datasetService, DatasetBucketDao datasetBucketDao) {
    this.datasetId = datasetId;
    this.datasetService = datasetService;
    this.datasetBucketDao = datasetBucketDao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    Dataset dataset = datasetService.retrieve(datasetId);
    Set<UUID> projectResourceIds = new HashSet();
    // main dataset google project
    projectResourceIds.add(dataset.getProjectResourceId());

    // google projects from ingests into dataset w/ different billing profile
    projectResourceIds.addAll(datasetBucketDao.getProjectResourceIdsForBucketPerDataset(datasetId));

    List<UUID> projectResourceIdList = new ArrayList<>(projectResourceIds);

    // Store all project resource ids
    workingMap.put(DatasetWorkingMapKeys.PROJECT_RESOURCE_ID_LIST, projectResourceIdList);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}

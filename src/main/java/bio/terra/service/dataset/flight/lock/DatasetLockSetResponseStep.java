package bio.terra.service.dataset.flight.lock;

import bio.terra.model.ResourceLocks;
import bio.terra.service.common.ResourceLockSetResponseStep;
import bio.terra.service.dataset.DatasetService;
import java.util.UUID;

public class DatasetLockSetResponseStep extends ResourceLockSetResponseStep {
  private final DatasetService datasetService;
  private final UUID datasetId;

  public DatasetLockSetResponseStep(DatasetService datasetService, UUID datasetId) {
    this.datasetService = datasetService;
    this.datasetId = datasetId;
  }

  @Override
  protected ResourceLocks getResourceLocks() {
    return datasetService.retrieveDatasetSummary(datasetId).getResourceLocks();
  }
}

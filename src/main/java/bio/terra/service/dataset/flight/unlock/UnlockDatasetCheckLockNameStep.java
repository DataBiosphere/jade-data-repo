package bio.terra.service.dataset.flight.unlock;

import bio.terra.service.common.UnlockResourceCheckLockNameStep;
import bio.terra.service.dataset.DatasetService;
import java.util.UUID;

public class UnlockDatasetCheckLockNameStep extends UnlockResourceCheckLockNameStep {
  private final DatasetService datasetService;
  private final UUID datasetId;

  public UnlockDatasetCheckLockNameStep(
      DatasetService datasetService, UUID datasetId, String lockName) {
    super(lockName);
    this.datasetService = datasetService;
    this.datasetId = datasetId;
  }

  protected String getExclusiveLock() {
    return datasetService.retrieveDatasetSummary(datasetId).getResourceLocks().getExclusive();
  }
}

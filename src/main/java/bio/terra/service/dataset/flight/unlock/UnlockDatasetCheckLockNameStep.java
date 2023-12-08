package bio.terra.service.dataset.flight.unlock;

import bio.terra.service.common.UnlockResourceCheckLockNameStep;
import bio.terra.service.dataset.DatasetService;
import java.util.ArrayList;
import java.util.List;
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

  protected List<String> getLocks() {
    var locks = new ArrayList<String>();
    var lockResource = datasetService.retrieveDatasetSummary(datasetId).getResourceLocks();
    if (lockResource.getExclusive() != null) {
      locks.add(lockResource.getExclusive());
    }
    locks.addAll(lockResource.getShared());
    return locks;
  }

  protected boolean isSharedLock(String lockName) {
    var lockResource = datasetService.retrieveDatasetSummary(datasetId).getResourceLocks();
    return lockResource.getShared().contains(lockName);
  }
}

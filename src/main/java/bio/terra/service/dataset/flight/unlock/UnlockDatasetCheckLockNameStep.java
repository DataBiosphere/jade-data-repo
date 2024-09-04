package bio.terra.service.dataset.flight.unlock;

import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.common.UnlockResourceCheckLockNameStep;
import bio.terra.service.dataset.DatasetService;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class UnlockDatasetCheckLockNameStep extends UnlockResourceCheckLockNameStep {
  private final DatasetService datasetService;

  public UnlockDatasetCheckLockNameStep(
      DatasetService datasetService, UUID datasetId, String lockName) {
    super(IamResourceType.DATASET, datasetId, lockName);
    this.datasetService = datasetService;
  }

  protected List<String> getLocks() {
    var locks = new ArrayList<String>();
    var lockResource = datasetService.retrieveDatasetSummary(resourceId).getResourceLocks();
    if (lockResource.getExclusive() != null) {
      locks.add(lockResource.getExclusive());
    }
    if (lockResource.getShared() != null) {
      locks.addAll(lockResource.getShared());
    }
    return locks;
  }

  protected boolean isSharedLock(String lockName) {
    var lockResource = datasetService.retrieveDatasetSummary(resourceId).getResourceLocks();
    if (lockResource.getShared() == null) {
      return false;
    }
    return lockResource.getShared().contains(lockName);
  }
}

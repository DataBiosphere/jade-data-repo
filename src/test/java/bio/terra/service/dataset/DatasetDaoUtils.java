package bio.terra.service.dataset;

import bio.terra.common.ResourceLocksUtils;
import bio.terra.model.ResourceLocks;
import java.util.UUID;

public class DatasetDaoUtils {

  /**
   * @return the dataset's exclusive lock obtained from its summary, or null if not present
   */
  public static String getExclusiveLock(DatasetDao datasetDao, UUID datasetId) {
    ResourceLocks resourceLocks = datasetDao.retrieveSummaryById(datasetId).getResourceLocks();
    return ResourceLocksUtils.getExclusiveLock(resourceLocks);
  }

  // accessing protected method to be used in other tests
  public String[] getSharedLocks(DatasetDao datasetDao, UUID datasetId) {
    return datasetDao.getSharedLocks(datasetId);
  }
}

package bio.terra.service.dataset.dao;

import java.util.UUID;

public class DatasetDaoUtils {

    // accessing protected method to be used in other tests
    public String[] getSharedLocks(DatasetDao datasetDao, UUID datasetId) {
        return datasetDao.getSharedLocks(datasetId);
    }
}

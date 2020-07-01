package bio.terra.service.dataset;

import org.springframework.beans.factory.annotation.Autowired;
import java.util.UUID;

public class DatasetDaoUtils {

    @Autowired
    private DatasetDao datasetDao;

    // accessing protected method to be used in other tests
    public String[] getSharedLocks(UUID datasetId) {
        return datasetDao.getSharedLocks(datasetId);
    }
}

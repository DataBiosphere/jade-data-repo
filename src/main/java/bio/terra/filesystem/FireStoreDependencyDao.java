package bio.terra.filesystem;

import bio.terra.metadata.FSObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
public class FireStoreDependencyDao {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.filesystem.FireStoreDependencyDao");

/*
    private Firestore firestore;

    @Autowired
    public FireStoreDependencyDao(Firestore firestore) {
        this.firestore = firestore;
    }
*/

    // TODO: Implement this
    public boolean hasDatasetReference(String studyId, String objectId) {
        return false;
    }

    public List<String> validateRefIds(String studyId, List<String> refIdArray, FSObject.FSObjectType objectType) {
        return new ArrayList<>();
    }

    public void storeDatasetFileDependencies(String studyId, String datasetId, List<String> refIds) {
    }

    public void deleteDatasetFileDependencies(String studyId, String datasetId) {
    }

}

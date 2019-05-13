package bio.terra.filesystem;

import bio.terra.metadata.FSObject;
import com.google.cloud.firestore.Firestore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Component
public class FireStoreDependencyDao {
    private final Logger logger = LoggerFactory.getLogger("bio.terra.filesystem.FireStoreDependencyDao");

    private Firestore firestore;

    @Autowired
    public FireStoreDependencyDao(Firestore firestore) {
        this.firestore = firestore;
    }


    // TODO: Implement this
    public boolean hasDatasetReference(UUID studyId, UUID objectId) {
        return false;
    }

    public List<String> validateRefIds(UUID studyId, List<String> refIdArray, FSObject.FSObjectType objectType) {
        return new ArrayList<String>();
    }

    public void storeDatasetFileDependencies(UUID studyId, UUID datasetId, List<String> refIds) {
    }

    public void deleteDatasetFileDependencies(UUID studyId, UUID datasetId) {
    }

}

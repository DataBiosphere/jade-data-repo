package bio.terra.service.filedata.google.firestore;

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public final class FireStoreProject {
    private static final Logger logger = LoggerFactory.getLogger(FireStoreProject.class);
    private static final ConcurrentHashMap<String, FireStoreProject> fireStoreProjectCache = new ConcurrentHashMap<>();

    private String projectId;
    private Firestore firestore;

    private FireStoreProject(String projectId) {
        this.projectId = projectId;
        this.firestore = FirestoreOptions.newBuilder()
            .setProjectId(projectId)
            .build()
            .getService();
    }

    String getProjectId() {
        return projectId;
    }

    Firestore getFirestore() {
        return firestore;
    }

    public static FireStoreProject get(String projectId) {
        // We use a concurrent hash map to make sure that two threads both do not create the project at
        // the same time and corrupt the map.  computeIfAbsent will avoid the relatively high cost of building then
        // tossing a firestore project if it already exists. It is a way to combine checking for the key and only
        // creating the firestore project if it is not present in the cache.
        fireStoreProjectCache.computeIfAbsent(projectId, FireStoreProject::new);
        return fireStoreProjectCache.get(projectId);
    }

}

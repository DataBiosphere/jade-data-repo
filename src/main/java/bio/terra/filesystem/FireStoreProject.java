package bio.terra.filesystem;

import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public final class FireStoreProject {
    private static final Logger logger = LoggerFactory.getLogger(FireStoreProject.class);
    private static ConcurrentHashMap<String, FireStoreProject> fireStoreProjectCache = new ConcurrentHashMap<>();

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
        // the same time and corrupt the map. The 'containsKey' is here to avoid the relatively high cost
        // of building and then tossing a firestore project if it already exists. The 'putIfAbsent' will
        // toss the project in the unlikely event of a collision on creating a filestore project for this
        // project id.
        if (!fireStoreProjectCache.containsKey(projectId)) {
            FireStoreProject fireStoreProject = new FireStoreProject(projectId);
            fireStoreProjectCache.putIfAbsent(projectId, fireStoreProject);
        }
        return fireStoreProjectCache.get(projectId);
    }

}

package bio.terra.filesystem;

import com.google.auth.Credentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

public class FireStoreProject {
    private static final Logger logger = LoggerFactory.getLogger(FireStoreProject.class);
    private static HashMap<ProjectAndCredential, FireStoreProject> fireStoreProjectLookup = new HashMap<>();
    private String projectId;
    private Firestore firestore;
    public FireStoreProject(String projectId) {
        logger.info("Retrieving firestore project for project id: {}", projectId);
        this.projectId = projectId;
        firestore = FirestoreOptions.newBuilder()
            .setProjectId(projectId)
            .build()
            .getService();
    }

    public FireStoreProject(String projectId, Credentials credentials) {
        this.projectId = projectId;
        firestore = FirestoreOptions.newBuilder()
            .setProjectId(projectId)
            .setCredentials(credentials)
            .build()
            .getService();
    }

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public Firestore getFirestore() {
        return firestore;
    }

    public void setFirestore(Firestore firestore) {
        this.firestore = firestore;
    }

    public static FireStoreProject get(String projectId, Credentials credentials) {
        ProjectAndCredential projectAndCredential = new ProjectAndCredential(projectId, credentials);
        if (!fireStoreProjectLookup.containsKey(projectAndCredential)) {
            FireStoreProject fireStoreProject;
            if (credentials == null) {
                fireStoreProject = new FireStoreProject(projectId);
            } else {
                fireStoreProject = new FireStoreProject(projectId, credentials);
            }
            fireStoreProjectLookup.put(projectAndCredential, fireStoreProject);
        }
        return fireStoreProjectLookup.get(projectAndCredential);
    }

    public static FireStoreProject get(String projectId) {
        return get(projectId, null);
    }
}

package bio.terra.filesystem;

import com.google.auth.Credentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.FirestoreOptions;

public class FireStoreProject {
    private String projectId;
    private Firestore firestore;
    public FireStoreProject(String projectId) {
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
}

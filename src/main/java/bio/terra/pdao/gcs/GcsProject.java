package bio.terra.pdao.gcs;

import com.google.auth.Credentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class GcsProject {
    private static final int defaultConnectTimeoutSeconds = 100;
    private static final int defaultReadTimeoutSeconds = 100;

    private final String projectId;
    private final Storage storage;

    public GcsProject(String projectId, Credentials credentials, int connectTimeoutSeconds, int readTimeoutSeconds) {
        this.projectId = projectId;
        HttpTransportOptions transportOptions = StorageOptions.getDefaultHttpTransportOptions();
        transportOptions = transportOptions.toBuilder()
            .setConnectTimeout(connectTimeoutSeconds * 1000)
            .setReadTimeout(readTimeoutSeconds * 1000)
            .build();
        StorageOptions storageOptions = StorageOptions.newBuilder()
            .setTransportOptions(transportOptions)
            .setProjectId(projectId)
            .setCredentials(credentials)
            .build();
        this.storage = storageOptions.getService();
    }

    public GcsProject(String projectId, Credentials credentials) {
        this(projectId, credentials, defaultConnectTimeoutSeconds, defaultReadTimeoutSeconds);
    }

    public String getProjectId() {
        return projectId;
    }

    public Storage getStorage() {
        return storage;
    }
}

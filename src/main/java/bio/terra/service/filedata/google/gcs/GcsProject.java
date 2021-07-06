package bio.terra.service.filedata.google.gcs;

import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class GcsProject {
  private final String projectId;
  private final Storage storage;

  GcsProject(String projectId, int connectTimeoutSeconds, int readTimeoutSeconds) {
    this.projectId = projectId;
    HttpTransportOptions transportOptions = StorageOptions.getDefaultHttpTransportOptions();
    transportOptions =
        transportOptions.toBuilder()
            .setConnectTimeout(connectTimeoutSeconds * 1000)
            .setReadTimeout(readTimeoutSeconds * 1000)
            .build();
    StorageOptions storageOptions =
        StorageOptions.newBuilder()
            .setTransportOptions(transportOptions)
            .setProjectId(projectId)
            .build();
    this.storage = storageOptions.getService();
  }

  public String getProjectId() {
    return projectId;
  }

  public Storage getStorage() {
    return storage;
  }
}

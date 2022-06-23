package bio.terra.service.filedata.google.gcs;

import bio.terra.service.filedata.google.gcs.GcsProjectFactory.ProjectUserIdentifier;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import com.google.api.services.iam.v1.IamScopes;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.time.Duration;
import java.util.List;

public class GcsProject {
  static final Duration TOKEN_LENGTH = Duration.ofMinutes(60);
  private final ProjectUserIdentifier projectUserIdentifier;
  private final Storage storage;

  GcsProject(
      ProjectUserIdentifier projectUserIdentifier,
      int connectTimeoutSeconds,
      int readTimeoutSeconds) {
    this.projectUserIdentifier = projectUserIdentifier;
    HttpTransportOptions transportOptions = StorageOptions.getDefaultHttpTransportOptions();
    transportOptions =
        transportOptions.toBuilder()
            .setConnectTimeout(connectTimeoutSeconds * 1000)
            .setReadTimeout(readTimeoutSeconds * 1000)
            .build();
    GoogleCredentials credentials;

    try {
      credentials = GoogleCredentials.getApplicationDefault();

      // Create a short-lived token that impersonated the specified service account
      if (projectUserIdentifier.userToImpersonate() != null) {
        credentials =
            ImpersonatedCredentials.create(
                credentials,
                projectUserIdentifier.userToImpersonate(),
                null,
                List.of(IamScopes.CLOUD_PLATFORM),
                (int) TOKEN_LENGTH.toSeconds());
      }
    } catch (IOException e) {
      throw new GoogleResourceException("Could not generate Google credentials", e);
    }

    StorageOptions storageOptions =
        StorageOptions.newBuilder()
            .setTransportOptions(transportOptions)
            .setProjectId(projectUserIdentifier.projectId())
            .setCredentials(credentials)
            .build();
    this.storage = storageOptions.getService();
  }

  public String getProjectId() {
    return projectUserIdentifier.projectId();
  }

  public Storage getStorage() {
    return storage;
  }
}

package bio.terra.integration;

import com.google.auth.Credentials;
import com.google.cloud.http.HttpTransportOptions;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public final class GcsFixtures {
  private static final int connectTimeoutSeconds = 100;
  private static final int readTimeoutSeconds = 100;

  private GcsFixtures() {}

  public static Storage getStorage(Credentials credentials) {
    HttpTransportOptions transportOptions =
        StorageOptions.getDefaultHttpTransportOptions().toBuilder()
            .setConnectTimeout(connectTimeoutSeconds * 1000)
            .setReadTimeout(readTimeoutSeconds * 1000)
            .build();
    return StorageOptions.newBuilder()
        .setTransportOptions(transportOptions)
        .setCredentials(credentials)
        .build()
        .getService();
  }
}

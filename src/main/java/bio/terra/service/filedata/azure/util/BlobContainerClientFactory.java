package bio.terra.service.filedata.azure.util;

import bio.terra.common.ValidationUtils;
import com.azure.core.credential.AzureSasCredential;
import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import java.time.Duration;
import java.util.Locale;
import java.util.Objects;

/**
 * A class that wraps the SDK container client and facilities the creation of SAS Urls for blobs by
 * abstracting the different strategies that depend on the authentication mechanism.
 */
public class BlobContainerClientFactory {

  public static final Duration DELEGATED_KEY_DURATION = Duration.ofHours(24);
  private final HttpClient httpClient = HttpClient.createDefault();
  private final int MAX_RETRIES = 3;
  private final int RETRY_TIMEOUT_SECONDS = 3600;
  private final RequestRetryOptions retryOptions =
      new RequestRetryOptions(
          RetryPolicyType.EXPONENTIAL, MAX_RETRIES, RETRY_TIMEOUT_SECONDS, null, null, null);

  private final BlobContainerClient blobContainerClient;

  public BlobSasUrlFactory getBlobSasUrlFactory() {
    return blobSasUrlFactory;
  }

  private final BlobSasUrlFactory blobSasUrlFactory;

  public BlobContainerClientFactory(String accountName, String accountKey, String containerName) {

    blobContainerClient =
        createBlobServiceClientUsingSharedKey(
                ValidationUtils.requireNotBlank(accountName, "Account name is null or empty"),
                ValidationUtils.requireNotBlank(accountKey, "Account key is null or empty"))
            .getBlobContainerClient(
                ValidationUtils.requireNotBlank(containerName, "Container name is null or empty"));
    blobSasUrlFactory = new SharedAccountKeySasUrlFactory(blobContainerClient);
  }

  public BlobContainerClientFactory(
      String accountName, TokenCredential azureCredential, String containerName) {

    var blobServiceClient =
        createBlobServiceClientUsingTokenCredentials(
            ValidationUtils.requireNotBlank(accountName, "Account name is null or empty"),
            Objects.requireNonNull(azureCredential, "Azure token credentials are null."));
    blobContainerClient =
        blobServiceClient.getBlobContainerClient(
            ValidationUtils.requireNotBlank(containerName, "Container name is null or empty"));

    // The delegated key expiration is set to a constant.
    // There is little benefit for the caller to adjust this value.
    // A long duration minimizes the number of calls to get it and noise in audits logs.
    blobSasUrlFactory =
        new UserDelegatedKeySasUrlFactory(blobServiceClient, containerName, DELEGATED_KEY_DURATION);
  }

  public BlobContainerClientFactory(String containerURLWithSasToken) {

    BlobUrlParts blobUrl = BlobUrlParts.parse(containerURLWithSasToken);

    blobContainerClient =
        new BlobContainerClientBuilder()
            .httpClient(httpClient)
            .endpoint(
                String.format(
                    Locale.ROOT,
                    "https://%s/%s",
                    blobUrl.getHost(),
                    blobUrl.getBlobContainerName()))
            .credential(new AzureSasCredential(blobUrl.getCommonSasQueryParameters().encode()))
            .retryOptions(retryOptions)
            .buildClient();

    blobSasUrlFactory = new ContainerSasTokenSasUrlFactory(blobUrl);
  }

  public BlobContainerClient getBlobContainerClient() {
    return blobContainerClient;
  }

  private BlobServiceClient createBlobServiceClientUsingSharedKey(
      String accountName, String accountKey) {
    return new BlobServiceClientBuilder()
        .credential(new StorageSharedKeyCredential(accountName, accountKey))
        .httpClient(httpClient)
        .endpoint(String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName))
        .retryOptions(retryOptions)
        .buildClient();
  }

  private BlobServiceClient createBlobServiceClientUsingTokenCredentials(
      String accountName, TokenCredential credentials) {
    return new BlobServiceClientBuilder()
        .credential(credentials)
        .httpClient(httpClient)
        .endpoint(String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName))
        .retryOptions(retryOptions)
        .buildClient();
  }
}

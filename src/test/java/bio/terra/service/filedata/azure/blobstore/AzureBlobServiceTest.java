package bio.terra.service.filedata.azure.blobstore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.filedata.azure.util.BlobContainerClientFactory;
import bio.terra.service.filedata.azure.util.BlobCrl;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.common.policy.RequestRetryOptions;
import java.time.Duration;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class AzureBlobServiceTest {
  @Mock private AzureResourceConfiguration resourceConfiguration;

  private static final int MAX_RETRIES = 2;
  private static final int RETRY_TIMEOUT_SECONDS = 5;

  private static final String ACCOUNT_NAME = "accountName";
  private static final String CONTAINER_NAME = "containerName";
  private static final String BLOB_NAME = "blobName";
  private static final String STORAGE_URL =
      "https://%s.blob.core.windows.net/%s".formatted(ACCOUNT_NAME, CONTAINER_NAME);
  private static final String SIGNED_URL =
      "https://%s.blob.core.windows.net/%s/%s?sig=SIGNATURE"
          .formatted(ACCOUNT_NAME, CONTAINER_NAME, BLOB_NAME);

  private AzureBlobService azureBlobService;

  @BeforeEach
  void setup() {
    azureBlobService = new AzureBlobService(resourceConfiguration);
    when(resourceConfiguration.maxRetries()).thenReturn(MAX_RETRIES);
    when(resourceConfiguration.retryTimeoutSeconds()).thenReturn(RETRY_TIMEOUT_SECONDS);
  }

  @Test
  void testGetRetryOptions() {
    RequestRetryOptions requestRetryOptions = azureBlobService.getRetryOptions();

    Duration expectedTimeout = Duration.ofSeconds(RETRY_TIMEOUT_SECONDS);
    assertThat(requestRetryOptions.getMaxTries(), equalTo(MAX_RETRIES));
    assertThat(requestRetryOptions.getTryTimeoutDuration(), equalTo(expectedTimeout));
  }

  @Test
  void testGetBlobCrl() {
    RequestRetryOptions retryOptions = azureBlobService.getRetryOptions();

    BlobContainerClientFactory destinationClientFactory =
        new BlobContainerClientFactory(SIGNED_URL, retryOptions);
    BlobCrl blobCrl = azureBlobService.getBlobCrl(destinationClientFactory);

    assertNotNull(blobCrl);
  }

  @Test
  void testGetSourceClientFactory() {
    BlobContainerClientFactory blobContainerClientFactory =
        azureBlobService.getSourceClientFactory(SIGNED_URL);
    BlobContainerClient blobContainerClient = blobContainerClientFactory.getBlobContainerClient();

    assertThat(blobContainerClient.getAccountName(), equalTo(ACCOUNT_NAME));
    assertThat(blobContainerClient.getBlobContainerUrl(), equalTo(STORAGE_URL));
    assertThat(blobContainerClient.getBlobContainerName(), equalTo(CONTAINER_NAME));
  }

  @Test
  void testGetSourceClientFactoryWithCredential() {
    TokenCredential azureCredential =
        new ClientSecretCredentialBuilder()
            .clientId("clientId")
            .clientSecret("clientSecret")
            .tenantId("tenantId")
            .build();

    BlobContainerClientFactory blobContainerClientFactory =
        azureBlobService.getSourceClientFactory(ACCOUNT_NAME, azureCredential, CONTAINER_NAME);
    BlobContainerClient blobContainerClient = blobContainerClientFactory.getBlobContainerClient();

    assertThat(blobContainerClient.getAccountName(), equalTo(ACCOUNT_NAME));
    assertThat(blobContainerClient.getBlobContainerUrl(), equalTo(STORAGE_URL));
    assertThat(blobContainerClient.getBlobContainerName(), equalTo(CONTAINER_NAME));
  }
}

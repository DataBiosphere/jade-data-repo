package bio.terra.service.filedata.azure.blobstore;

import bio.terra.service.filedata.azure.util.BlobContainerClientFactory;
import bio.terra.service.filedata.azure.util.BlobCrl;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.core.credential.TokenCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AzureBlobService {
  private final AzureResourceConfiguration resourceConfiguration;

  @Autowired
  public AzureBlobService(AzureResourceConfiguration resourceConfiguration) {
    this.resourceConfiguration = resourceConfiguration;
  }

  public RequestRetryOptions getRetryOptions() {
    return new RequestRetryOptions(
        RetryPolicyType.EXPONENTIAL,
        this.resourceConfiguration.maxRetries(),
        this.resourceConfiguration.retryTimeoutSeconds(),
        null,
        null,
        null);
  }

  public BlobCrl getBlobCrl(BlobContainerClientFactory destinationClientFactory) {
    return new BlobCrl(destinationClientFactory);
  }

  public BlobContainerClientFactory getSourceClientFactory(String url) {
    return new BlobContainerClientFactory(url, getRetryOptions());
  }

  public BlobContainerClientFactory getSourceClientFactory(
      String accountName, TokenCredential azureCredential, String containerName) {
    return new BlobContainerClientFactory(
        accountName, azureCredential, containerName, getRetryOptions());
  }
}

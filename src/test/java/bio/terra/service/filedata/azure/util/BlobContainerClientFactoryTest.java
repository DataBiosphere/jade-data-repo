package bio.terra.service.filedata.azure.util;

import static bio.terra.service.filedata.azure.util.AzureBlobIOTestUtility.MIB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.core.credential.TokenCredential;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * Note: To run these tests the following must be set:
 *
 * <UL>
 *   <LI>AZURE_CREDENTIALS_APPLICATIONID
 *   <LI>AZURE_CREDENTIALS_SECRET
 * </UL>
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class BlobContainerClientFactoryTest {

  @Autowired private AzureResourceConfiguration azureResourceConfiguration;

  @Autowired private ConnectedTestConfiguration connectedTestConfiguration;

  private AzureBlobIOTestUtility blobIOTestUtility;
  private String accountName;
  private String containerName;
  private String blobName;
  private TokenCredential tokenCredential;
  private RequestRetryOptions retryOptions;

  @Before
  public void setUp() {
    retryOptions =
        new RequestRetryOptions(
            RetryPolicyType.EXPONENTIAL,
            azureResourceConfiguration.getMaxRetries(),
            azureResourceConfiguration.getRetryTimeoutSeconds(),
            null,
            null,
            null);
    blobIOTestUtility =
        new AzureBlobIOTestUtility(
            azureResourceConfiguration.getAppToken(connectedTestConfiguration.getTargetTenantId()),
            connectedTestConfiguration.getSourceStorageAccountName(),
            connectedTestConfiguration.getDestinationStorageAccountName(),
            retryOptions);

    accountName = connectedTestConfiguration.getSourceStorageAccountName();
    tokenCredential =
        azureResourceConfiguration.getAppToken(connectedTestConfiguration.getTargetTenantId());
    containerName = blobIOTestUtility.getSourceBlobContainerClient().getBlobContainerName();
    blobName = blobIOTestUtility.uploadSourceFiles(1, MIB / 10).iterator().next();
  }

  @After
  public void tearDown() throws Exception {
    blobIOTestUtility.teardown();
  }

  @Test
  public void testCreateReadOnlySasUrlForBlobUsingTokenCreds_UrlIsGeneratedAndIsValid() {

    BlobContainerClientFactory factory =
        new BlobContainerClientFactory(accountName, tokenCredential, containerName, retryOptions);

    BlobClient blobClient =
        getBlobClientFromUrl(
            factory
                .getBlobSasUrlFactory()
                .createSasUrlForBlob(blobName, blobIOTestUtility.createReadOnlyTokenOptions()));

    assertThat(blobClient.exists(), is(true));
  }

  @Test
  public void testCreateReadOnlySasUrlForBlobUsingContainerSasCreds_UrlIsGeneratedAndIsValid() {

    BlobContainerClientFactory factory =
        new BlobContainerClientFactory(
            blobIOTestUtility.generateSourceContainerUrlWithSasReadAndListPermissions(
                getSourceStorageAccountPrimarySharedKey()),
            retryOptions);

    BlobClient blobClient =
        getBlobClientFromUrl(
            factory
                .getBlobSasUrlFactory()
                .createSasUrlForBlob(blobName, blobIOTestUtility.createReadOnlyTokenOptions()));

    assertThat(blobClient.exists(), is(true));
  }

  @Test
  public void testCreateReadOnlySasUrlForBlobUsingSharedKeyCreds_UrlIsGeneratedAndIsValid() {

    BlobContainerClientFactory factory =
        new BlobContainerClientFactory(
            accountName, getSourceStorageAccountPrimarySharedKey(), containerName, retryOptions);

    BlobClient blobClient =
        getBlobClientFromUrl(
            factory
                .getBlobSasUrlFactory()
                .createSasUrlForBlob(blobName, blobIOTestUtility.createReadOnlyTokenOptions()));

    assertThat(blobClient.exists(), is(true));
  }

  private BlobClient getBlobClientFromUrl(String blobUrl) {
    BlobUrlParts parts = BlobUrlParts.parse(blobUrl);

    return new BlobClientBuilder().endpoint(parts.toUrl().toString()).buildClient();
  }

  private String getSourceStorageAccountPrimarySharedKey() {
    AzureResourceManager client =
        azureResourceConfiguration.getClient(
            connectedTestConfiguration.getTargetTenantId(),
            connectedTestConfiguration.getTargetSubscriptionId());

    return client
        .storageAccounts()
        .getByResourceGroup(
            connectedTestConfiguration.getTargetResourceGroupName(),
            connectedTestConfiguration.getSourceStorageAccountName())
        .getKeys()
        .iterator()
        .next()
        .value();
  }
}

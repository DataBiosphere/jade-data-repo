package bio.terra.service.filedata.azure.util;

import static bio.terra.service.filedata.azure.util.BlobIOTestUtility.MiB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.core.credential.TokenCredential;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
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
 *   <LI>AZURE_CREDENTIALS_HOMETENANTID
 *   <LI>AZURE_CREDENTIALS_SECRET
 * </UL>
 *
 * Where AZURE_CREDENTIALS_HOMETENANTID must be the tenant of the source and destination storage
 * accounts.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class BlobContainerClientFactoryTest {

  @Autowired private AzureResourceConfiguration azureResourceConfiguration;

  @Autowired private ConnectedTestConfiguration connectedTestConfiguration;

  private BlobIOTestUtility blobIOTestUtility;
  private String accountName;
  private String containerName;
  private String blobName;
  private TokenCredential tokenCredential;

  @Before
  public void setUp() {

    blobIOTestUtility =
        new BlobIOTestUtility(
            azureResourceConfiguration.getAppToken(),
            connectedTestConfiguration.getSourceStorageAccountName(),
            connectedTestConfiguration.getDestinationStorageAccountName());

    accountName = connectedTestConfiguration.getSourceStorageAccountName();
    tokenCredential = azureResourceConfiguration.getAppToken();
    containerName = blobIOTestUtility.getSourceBlobContainerClient().getBlobContainerName();
    blobName = blobIOTestUtility.uploadSourceFiles(1, MiB / 10).stream().findFirst().get();
  }

  @After
  public void tearDown() throws Exception {
    blobIOTestUtility.deleteContainers();
  }

  @Test
  public void testCreateReadOnlySASUrlForBlobUsingTokenCreds_URLIsGeneratedAndIsValid() {

    BlobContainerClientFactory factory =
        new BlobContainerClientFactory(accountName, tokenCredential, containerName);

    BlobClient blobClient = getBlobClientFromUrl(factory.createReadOnlySASUrlForBlob(blobName));

    assertThat(blobClient.exists(), is(true));
  }

  @Test
  public void testCreateReadOnlySASUrlForBlobUsingContainerSasCreds_URLIsGeneratedAndIsValid() {

    BlobContainerClientFactory factory =
        new BlobContainerClientFactory(
            blobIOTestUtility.generateSourceContainerUrlWithSasReadAndListPermissions(
                getSourceStorageAccountPrimarySharedKey()));

    BlobClient blobClient = getBlobClientFromUrl(factory.createReadOnlySASUrlForBlob(blobName));

    assertThat(blobClient.exists(), is(true));
  }

  @Test
  public void testCreateReadOnlySASUrlForBlobUsingSharedKeyCreds_URLIsGeneratedAndIsValid() {

    BlobContainerClientFactory factory =
        new BlobContainerClientFactory(
            accountName, getSourceStorageAccountPrimarySharedKey(), containerName);

    BlobClient blobClient = getBlobClientFromUrl(factory.createReadOnlySASUrlForBlob(blobName));

    assertThat(blobClient.exists(), is(true));
  }

  private BlobClient getBlobClientFromUrl(String blobUrl) {
    BlobUrlParts parts = BlobUrlParts.parse(blobUrl);

    BlobClient blobClient =
        new BlobClientBuilder().endpoint(parts.toUrl().toString()).buildClient();
    return blobClient;
  }

  private String getSourceStorageAccountPrimarySharedKey() {
    AzureResourceManager client =
        this.azureResourceConfiguration.getClient(
            this.connectedTestConfiguration.getTargetSubscriptionId());

    return client
        .storageAccounts()
        .getByResourceGroup(
            this.connectedTestConfiguration.getTargetResourceGroupName(),
            this.connectedTestConfiguration.getSourceStorageAccountName())
        .getKeys()
        .stream()
        .findFirst()
        .get()
        .value();
  }
}

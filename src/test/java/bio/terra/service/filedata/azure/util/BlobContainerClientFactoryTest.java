package bio.terra.service.filedata.azure.util;

import static bio.terra.service.filedata.azure.util.BlobIOTestUtility.MIB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.core.credential.TokenCredential;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.models.BlobStorageException;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
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
            azureResourceConfiguration.getAppToken(connectedTestConfiguration.getTargetTenantId()),
            connectedTestConfiguration.getSourceStorageAccountName(),
            connectedTestConfiguration.getDestinationStorageAccountName());

    accountName = connectedTestConfiguration.getSourceStorageAccountName();
    tokenCredential =
        azureResourceConfiguration.getAppToken(connectedTestConfiguration.getTargetTenantId());
    containerName = blobIOTestUtility.getSourceBlobContainerClient().getBlobContainerName();
    blobName = blobIOTestUtility.uploadSourceFiles(1, MIB / 10).iterator().next();
  }

  @After
  public void tearDown() throws Exception {
    blobIOTestUtility.deleteContainers();
  }

  @Test
  public void testCreateReadOnlySasUrlForBlobUsingTokenCreds_UrlIsGeneratedAndIsValid() {

    BlobContainerClientFactory factory =
        new BlobContainerClientFactory(accountName, tokenCredential, containerName);

    BlobClient blobClient =
        getBlobClientFromUrl(factory.createReadOnlySasUrlForBlob(blobName, null, null));

    assertThat(blobClient.exists(), is(true));
  }

  @Test
  public void testCreateReadOnlySasUrlForBlobUsingContainerSasCreds_UrlIsGeneratedAndIsValid() {

    BlobContainerClientFactory factory =
        new BlobContainerClientFactory(
            blobIOTestUtility.generateSourceContainerUrlWithSasReadAndListPermissions(
                getSourceStorageAccountPrimarySharedKey()));

    BlobClient blobClient =
        getBlobClientFromUrl(factory.createReadOnlySasUrlForBlob(blobName, null, null));

    assertThat(blobClient.exists(), is(true));
  }

  @Test
  public void testCreateReadOnlySasUrlForBlobUsingSharedKeyCreds_UrlIsGeneratedAndIsValid() {

    BlobContainerClientFactory factory =
        new BlobContainerClientFactory(
            accountName, getSourceStorageAccountPrimarySharedKey(), containerName);

    BlobClient blobClient =
        getBlobClientFromUrl(factory.createReadOnlySasUrlForBlob(blobName, null, null));

    assertThat(blobClient.exists(), is(true));
  }

  @Test
  public void testCreateReadOnlySasUrlForBlobUsingSharedKeyCreds_CanModifySignature() {

    BlobContainerClientFactory factory =
        new BlobContainerClientFactory(
            accountName, getSourceStorageAccountPrimarySharedKey(), containerName);

    assertThat(
        "No content disposition encoded when null",
        BlobUrlParts.parse(factory.createReadOnlySasUrlForBlob(blobName, null, null))
            .getCommonSasQueryParameters()
            .getContentDisposition(),
        nullValue());

    assertThat(
        "Content disposition encoded is encoded",
        BlobUrlParts.parse(factory.createReadOnlySasUrlForBlob(blobName, "a@a.com", null))
            .getCommonSasQueryParameters()
            .getContentDisposition(),
        equalTo("a@a.com"));

    // Make sure URL works
    BlobClient blobClientGood =
        getBlobClientFromUrl(factory.createReadOnlySasUrlForBlob(blobName, "a@a.com", null));
    assertTrue("Encoded URL works", blobClientGood.exists());

    // Make sure URL is tamper-proof
    String badUrl =
        factory
            .createReadOnlySasUrlForBlob(blobName, "a@a.com", null)
            .replace("rscd=a%40a.com", "rscd=b%40b.com");
    assertThrows(
        "Can't tamper with token",
        BlobStorageException.class,
        () -> getBlobClientFromUrl(badUrl).exists());

    // Make sure url expires
    String expiredUrl = factory.createReadOnlySasUrlForBlob(blobName, null, Duration.ofSeconds(0));

    try {
      TimeUnit.SECONDS.sleep(5);
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    assertThrows(
        "Token expires",
        BlobStorageException.class,
        () -> getBlobClientFromUrl(expiredUrl).exists());
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

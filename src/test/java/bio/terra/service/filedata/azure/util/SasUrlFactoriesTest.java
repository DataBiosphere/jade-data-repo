package bio.terra.service.filedata.azure.util;

import static bio.terra.service.filedata.azure.util.AzureBlobIOTestUtility.MIB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.core.util.BinaryData;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.BlobServiceClientBuilder;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.UUID;
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

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class SasUrlFactoriesTest {
  @Autowired private AzureResourceConfiguration azureResourceConfiguration;

  @Autowired private ConnectedTestConfiguration connectedTestConfiguration;

  private AzureBlobIOTestUtility blobIOTestUtility;
  private String accountName;
  private String containerName;
  private String blobName;
  private List<BlobSasUrlFactory> sasUrlFactories;

  @Before
  public void setUp() {
    RequestRetryOptions retryOptions =
        new RequestRetryOptions(
            RetryPolicyType.EXPONENTIAL,
            azureResourceConfiguration.maxRetries(),
            azureResourceConfiguration.retryTimeoutSeconds(),
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
    containerName = blobIOTestUtility.getSourceBlobContainerClient().getBlobContainerName();
    BlobSasUrlFactory sharedKeyFactory =
        new SharedAccountKeySasUrlFactory(createBlobContainerClientUsingSharedKeyCredentials());
    BlobSasUrlFactory userDelegatedKeyFactory =
        new UserDelegatedKeySasUrlFactory(
            createBlobServiceClientUsingTokenCredentials(), containerName, Duration.ofHours(1));
    sasUrlFactories = List.of(sharedKeyFactory, userDelegatedKeyFactory);
  }

  @After
  public void tearDown() {
    blobIOTestUtility.teardown();
  }

  @Test
  public void testCreateSasTokenGeneratedWithReadPermission() {
    sasUrlFactories.forEach(this::testCreateSasTokenGeneratedWithReadPermission);
  }

  @Test
  public void testCreateSasTokenGeneratedWithCreatePermissions() {
    sasUrlFactories.forEach(this::testCreateSasTokenGeneratedWithCreatePermissions);
  }

  @Test
  public void testCreateSasTokenGeneratedWithDeletePermissions() {
    sasUrlFactories.forEach(this::testCreateSasTokenGeneratedWithDeletePermissions);
  }

  public void testCreateSasTokenGeneratedWithReadPermission(BlobSasUrlFactory factory) {
    blobName = blobIOTestUtility.uploadSourceFiles(1, MIB / 10).iterator().next();
    BlobSasPermission permission = new BlobSasPermission().setReadPermission(true);

    String sasUrl = factory.createSasUrlForBlob(blobName, createBlobSasTokenOptions(permission));

    BlobClient blobClient = createBlobClientUsingSasToken(sasUrl);

    assertThat(blobClient.getProperties().getBlobSize(), equalTo(MIB / 10));
  }

  public void testCreateSasTokenGeneratedWithWritePermissions(BlobSasUrlFactory factory) {
    blobName = blobIOTestUtility.uploadSourceFiles(1, MIB / 10).iterator().next();
    BlobSasPermission permission = new BlobSasPermission().setWritePermission(true);

    String sasUrl = factory.createSasUrlForBlob(blobName, createBlobSasTokenOptions(permission));

    // write permissions allow overwrite.
    String contentData = uploadContentDataToBlobUsingSasUrl(sasUrl, true);

    assertThat(
        getBlobClientWithFullAccess().getProperties().getBlobSize(),
        equalTo((long) contentData.length()));
  }

  public void testCreateSasTokenGeneratedWithCreatePermissions(BlobSasUrlFactory factory) {
    blobName = "myCreateTestBlob" + UUID.randomUUID(); // create does not allow overwrite
    BlobSasPermission permission = new BlobSasPermission().setCreatePermission(true);

    String sasUrl = factory.createSasUrlForBlob(blobName, createBlobSasTokenOptions(permission));
    // create does not allow overwrite
    String contentData = uploadContentDataToBlobUsingSasUrl(sasUrl, false);

    assertThat(
        getBlobClientWithFullAccess().getProperties().getBlobSize(),
        equalTo((long) contentData.length()));
  }

  public void testCreateSasTokenGeneratedWithDeletePermissions(BlobSasUrlFactory factory) {
    blobName = blobIOTestUtility.uploadSourceFiles(1, MIB / 10).iterator().next();
    BlobSasPermission permission = new BlobSasPermission().setDeletePermission(true);

    String sasUrl = factory.createSasUrlForBlob(blobName, createBlobSasTokenOptions(permission));

    BlobClient blobClient = createBlobClientUsingSasToken(sasUrl);
    blobClient.delete();

    assertThat(getBlobClientWithFullAccess().exists(), equalTo(false));
  }

  private String uploadContentDataToBlobUsingSasUrl(String sasUrl, Boolean overwrite) {
    BlobClient blobClient = createBlobClientUsingSasToken(sasUrl);
    String contentData = "0123456789";
    BinaryData data = BinaryData.fromString(contentData);
    blobClient.upload(data, overwrite);
    return contentData;
  }

  private BlobSasTokenOptions createBlobSasTokenOptions(BlobSasPermission permission) {
    return new BlobSasTokenOptions(Duration.ofHours(1), permission, "");
  }

  private BlobClient createBlobClientUsingSasToken(String sasUrl) {
    return new BlobClientBuilder().endpoint(sasUrl).buildClient();
  }

  private BlobContainerClient createBlobContainerClientUsingSharedKeyCredentials() {
    AzureResourceManager client =
        azureResourceConfiguration.getClient(
            connectedTestConfiguration.getTargetTenantId(),
            connectedTestConfiguration.getTargetSubscriptionId());

    String sharedKey =
        blobIOTestUtility.getSourceStorageAccountPrimarySharedKey(
            client, connectedTestConfiguration.getTargetResourceGroupName(), accountName);

    return new BlobServiceClientBuilder()
        .credential(new StorageSharedKeyCredential(accountName, sharedKey))
        .endpoint(getAccountEndpoint())
        .buildClient()
        .getBlobContainerClient(containerName);
  }

  private BlobClient getBlobClientWithFullAccess() {
    return blobIOTestUtility.getSourceBlobContainerClient().getBlobClient(blobName);
  }

  private BlobServiceClient createBlobServiceClientUsingTokenCredentials() {

    return new BlobServiceClientBuilder()
        .endpoint(getAccountEndpoint())
        .credential(
            azureResourceConfiguration.getAppToken(connectedTestConfiguration.getTargetTenantId()))
        .buildClient();
  }

  private String getAccountEndpoint() {
    return String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName);
  }

  private String getBlobEndpoint() {
    return String.format(
        Locale.ROOT,
        "https://%s.blob.core.windows.net/%s/%s",
        accountName,
        containerName,
        blobName);
  }
}

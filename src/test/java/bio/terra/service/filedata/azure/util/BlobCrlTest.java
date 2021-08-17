package bio.terra.service.filedata.azure.util;

import static bio.terra.service.filedata.azure.util.BlobIOTestUtility.MIB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import java.time.Duration;
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
public class BlobCrlTest {

  @Autowired private AzureResourceConfiguration azureResourceConfiguration;

  @Autowired private ConnectedTestConfiguration connectedTestConfiguration;

  private BlobIOTestUtility blobIOTestUtility;

  private BlobCrl blobCrl;

  @Before
  public void setUp() throws Exception {
    RequestRetryOptions retryOptions =
        new RequestRetryOptions(
            RetryPolicyType.EXPONENTIAL,
            azureResourceConfiguration.getMaxRetries(),
            azureResourceConfiguration.getRetryTimeoutSeconds(),
            null,
            null,
            null);
    blobIOTestUtility =
        new BlobIOTestUtility(
            azureResourceConfiguration.getAppToken(connectedTestConfiguration.getTargetTenantId()),
            connectedTestConfiguration.getSourceStorageAccountName(),
            connectedTestConfiguration.getDestinationStorageAccountName(),
            retryOptions);
    blobCrl = new BlobCrl(blobIOTestUtility.createDestinationClientFactory());
  }

  @After
  public void cleanUp() {
    blobIOTestUtility.deleteContainers();
  }

  @Test
  public void testDeleteBlob_BlobDoesNotExists() {
    String blobName = "myBlob";
    blobIOTestUtility.uploadDestinationFile(blobName, MIB / 10);

    blobCrl.deleteBlob(blobName);

    assertThat(
        blobIOTestUtility.getDestinationBlobContainerClient().getBlobClient(blobName).exists(),
        is(false));
  }

  @Test
  public void testGetBlobProperties_SizeInPropertiesMatches() {
    String blobName = "myBlob";
    blobIOTestUtility.uploadDestinationFile(blobName, MIB / 10);

    BlobProperties properties = blobCrl.getBlobProperties(blobName);

    assertThat(properties.getBlobSize(), equalTo(MIB / 10));
  }

  @Test
  public void
      testCreateSASUrlWithContentDisposition_SizeCanBeReadAndContentDispositionInSasToken() {
    String blobName = "myBlob";
    String contentDisposition = "myuser@foo.org";
    blobIOTestUtility.uploadDestinationFile(blobName, MIB / 10);

    BlobSasTokenOptions options =
        new BlobSasTokenOptions(
            Duration.ofMinutes(15),
            new BlobSasPermission().setReadPermission(true),
            contentDisposition);
    String sasUrl = blobCrl.createSasTokenUrlForBlob(blobName, options);

    BlobClient blobClient = new BlobClientBuilder().endpoint(sasUrl).buildClient();

    assertThat(blobClient.getProperties().getBlobSize(), equalTo(MIB / 10));
    assertThat(
        BlobUrlParts.parse(sasUrl).getCommonSasQueryParameters().getContentDisposition(),
        equalTo(contentDisposition));
  }
}

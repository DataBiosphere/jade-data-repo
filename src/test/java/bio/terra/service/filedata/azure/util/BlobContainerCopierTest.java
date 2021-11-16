package bio.terra.service.filedata.azure.util;

import static bio.terra.service.filedata.azure.util.BlobIOTestUtility.MIB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.policy.RetryPolicyType;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
public class BlobContainerCopierTest {

  @Autowired private AzureResourceConfiguration azureResourceConfiguration;

  @Autowired private ConnectedTestConfiguration connectedTestConfiguration;

  private static final int POLL_INTERVAL_IN_SECONDS = 2;
  private BlobIOTestUtility blobIOTestUtility;
  private Stream<BlobContainerCopier> copiersStream;
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
        new BlobIOTestUtility(
            azureResourceConfiguration.getAppToken(connectedTestConfiguration.getTargetTenantId()),
            connectedTestConfiguration.getSourceStorageAccountName(),
            connectedTestConfiguration.getDestinationStorageAccountName(),
            retryOptions);

    Duration pollDuration = Duration.ofSeconds(POLL_INTERVAL_IN_SECONDS);
    BlobContainerCopier copierWithSharedKeyCredentials =
        createBlobCopierWithSharedKeyInSource(pollDuration);
    BlobContainerCopier copierWithSasCredentials =
        createBlobCopierWithSasCredsInSource(pollDuration);
    BlobContainerCopier copierWithTokenCredentials =
        createBlobCopierWithTokenCredsInSource(pollDuration);

    copiersStream =
        Stream.of(
            copierWithSharedKeyCredentials, copierWithTokenCredentials, copierWithSasCredentials);
  }

  @After
  public void cleanUp() {
    blobIOTestUtility.deleteContainers();
  }
  /*
  Tests in this class should be parameterized tests. However, since we are using
  JUnit 4 the implementation would be convoluted. Instead, the execution pattern
  followed here should make it easier to migrate to JUnit 5 when appropriate.

  Migration approach:
  - Make each of the _Impl a parameterized test - @ParameterizedTest annotation
  - Convert the Stream of copiers to a method source.
   */

  @Test
  public void testCopySingleFile_FileIsCopied() {
    copiersStream.forEach(this::testCopySingleFile_FileIsCopied_Impl);
  }

  private void testCopySingleFile_FileIsCopied_Impl(BlobContainerCopier copier) {
    String blobName = blobIOTestUtility.uploadSourceFiles(1, MIB / 10).iterator().next();

    BlobCopySourceDestinationPair pair = new BlobCopySourceDestinationPair(blobName, "");
    copier.setSourceDestinationPairs(Collections.singletonList(pair));

    BlobContainerCopySyncPoller poller = copier.beginCopyOperation();

    poller.waitForCompletion();

    assertThat(
        blobIOTestUtility.getDestinationBlobContainerClient().getBlobClient(blobName).exists(),
        is(true));
  }

  @Test
  public void testCopy2FilesWithDestinationNames_FilesAreCopiedWithDestinationNames() {
    copiersStream.forEach(
        this::testCopy2FilesWithDestinationNames_FilesAreCopiedWithDestinationNames_Impl);
  }

  private void testCopy2FilesWithDestinationNames_FilesAreCopiedWithDestinationNames_Impl(
      BlobContainerCopier copier) {
    List<String> blobs = blobIOTestUtility.uploadSourceFiles(2, MIB / 10);

    List<BlobCopySourceDestinationPair> sourceDestinationPairs =
        blobs.stream()
            .map(b -> new BlobCopySourceDestinationPair(b, "foo" + b))
            .collect(Collectors.toList());

    copier.setSourceDestinationPairs(sourceDestinationPairs);

    BlobContainerCopySyncPoller poller = copier.beginCopyOperation();

    poller.waitForCompletion();

    sourceDestinationPairs.forEach(
        b ->
            assertThat(
                blobIOTestUtility
                    .getDestinationBlobContainerClient()
                    .getBlobClient(b.getDestinationBlobName())
                    .exists(),
                is(true)));
  }

  @Test
  public void testCopy5FilesUsingEmptyPrefix_AllFilesAreCopied() {
    copiersStream.forEach(this::testCopy5FilesUsingEmptyPrefix_AllFilesAreCopied_Impl);
  }

  private void testCopy5FilesUsingEmptyPrefix_AllFilesAreCopied_Impl(BlobContainerCopier copier) {
    List<String> blobs = blobIOTestUtility.uploadSourceFiles(5, MIB / 10);

    copier.setBlobSourcePrefix("");

    BlobContainerCopySyncPoller poller = copier.beginCopyOperation();

    poller.waitForCompletion();

    blobs.forEach(
        b ->
            assertThat(
                blobIOTestUtility.getDestinationBlobContainerClient().getBlobClient(b).exists(),
                is(true)));
  }

  @Test
  public void testCopySingleBlobWithSignedURl_FileIsCopied() {
    String blobName = blobIOTestUtility.uploadSourceFiles(1, MIB / 10).iterator().next();

    BlobContainerClientFactory sourceFactory = createSourceClientFactoryWithSharedKey();

    BlobContainerCopier copier =
        new BlobContainerCopier(blobIOTestUtility.createDestinationClientFactory());
    copier.setSourceBlobUrl(
        sourceFactory
            .getBlobSasUrlFactory()
            .createSasUrlForBlob(blobName, blobIOTestUtility.createReadOnlyTokenOptions()));

    copier.beginCopyOperation().waitForCompletion();

    assertThat(
        blobIOTestUtility.getDestinationBlobContainerClient().getBlobClient(blobName).exists(),
        is(true));
  }

  @Test
  public void testCopySingleBLobWithSignedURlAndDestinationName_FileCopiedWithDestinationName() {
    String blobName = blobIOTestUtility.uploadSourceFiles(1, MIB / 10).iterator().next();
    String destinationBlobName = "myBlob";
    BlobContainerClientFactory sourceFactory = createSourceClientFactoryWithSharedKey();

    BlobContainerCopier copier =
        new BlobContainerCopier(blobIOTestUtility.createDestinationClientFactory());
    copier.setSourceBlobUrl(
        sourceFactory
            .getBlobSasUrlFactory()
            .createSasUrlForBlob(blobName, blobIOTestUtility.createReadOnlyTokenOptions()));
    copier.setDestinationBlobName(destinationBlobName);
    copier.beginCopyOperation().waitForCompletion();

    assertThat(
        blobIOTestUtility
            .getDestinationBlobContainerClient()
            .getBlobClient(destinationBlobName)
            .exists(),
        is(true));
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

  private BlobContainerCopier createBlobCopierWithSharedKeyInSource(Duration pollingInterval) {

    BlobContainerCopier copier =
        new BlobContainerCopier(this.blobIOTestUtility.createDestinationClientFactory());
    copier.setPollingInterval(pollingInterval);

    copier.setSourceClientFactory(createSourceClientFactoryWithSharedKey());

    return copier;
  }

  private BlobContainerClientFactory createSourceClientFactoryWithSharedKey() {
    String sourceContainer =
        blobIOTestUtility.getSourceBlobContainerClient().getBlobContainerName();
    return new BlobContainerClientFactory(
        connectedTestConfiguration.getSourceStorageAccountName(),
        getSourceStorageAccountPrimarySharedKey(),
        sourceContainer,
        retryOptions);
  }

  private BlobContainerCopier createBlobCopierWithTokenCredsInSource(Duration pollingInterval) {

    BlobContainerCopier copier =
        new BlobContainerCopier(blobIOTestUtility.createDestinationClientFactory());
    copier.setPollingInterval(pollingInterval);
    String sourceContainer =
        blobIOTestUtility.getSourceBlobContainerClient().getBlobContainerName();
    copier.setSourceClientFactory(
        new BlobContainerClientFactory(
            connectedTestConfiguration.getSourceStorageAccountName(),
            azureResourceConfiguration.getAppToken(connectedTestConfiguration.getTargetTenantId()),
            sourceContainer,
            retryOptions));

    return copier;
  }

  private BlobContainerCopier createBlobCopierWithSasCredsInSource(Duration pollingInterval) {

    BlobContainerCopier copier =
        new BlobContainerCopier(blobIOTestUtility.createDestinationClientFactory());
    copier.setPollingInterval(pollingInterval);
    copier.setSourceClientFactory(createSourceClientFactoryWithSasCreds());

    return copier;
  }

  private BlobContainerClientFactory createSourceClientFactoryWithSasCreds() {
    return new BlobContainerClientFactory(
        blobIOTestUtility.generateSourceContainerUrlWithSasReadAndListPermissions(
            getSourceStorageAccountPrimarySharedKey()),
        retryOptions);
  }
}

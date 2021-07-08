package bio.terra.service.filedata.azure.util;

import static bio.terra.service.filedata.azure.util.BlobIOTestUtility.MiB;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.resourcemanager.AzureResourceManager;
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
public class BlobContainerCopierTest {

  @Autowired private AzureResourceConfiguration azureResourceConfiguration;

  @Autowired private ConnectedTestConfiguration connectedTestConfiguration;

  private static final int POLL_INTERVAL_IN_SECONDS = 2;
  private BlobIOTestUtility blobIOTestUtility;
  private Stream<BlobContainerCopier> copiersStream;

  @Before
  public void setUp() {

    blobIOTestUtility =
        new BlobIOTestUtility(
            azureResourceConfiguration.getAppToken(),
            connectedTestConfiguration.getSourceStorageAccountName(),
            connectedTestConfiguration.getDestinationStorageAccountName());

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
    String blobName = blobIOTestUtility.uploadSourceFiles(1, MiB / 10).stream().findFirst().get();

    BlobCopySourceDestinationPair pair = new BlobCopySourceDestinationPair(blobName, "");
    copier.setSourceDestinationPairs(Collections.singletonList(pair));

    BlobContainerCopySyncPoller poller = copier.beginCopyOperation();

    poller.waitForCompletion();

    assertThat(
        this.blobIOTestUtility.getDestinationBlobContainerClient().getBlobClient(blobName).exists(),
        is(true));
  }

  @Test
  public void testCopy2FilesWithDestinationNames_FilesAreCopiedWithDestinationNames() {
    copiersStream.forEach(
        this::testCopy2FilesWithDestinationNames_FilesAreCopiedWithDestinationNames_Impl);
  }

  private void testCopy2FilesWithDestinationNames_FilesAreCopiedWithDestinationNames_Impl(
      BlobContainerCopier copier) {
    List<String> blobs = blobIOTestUtility.uploadSourceFiles(2, MiB / 10);

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
                this.blobIOTestUtility
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
    List<String> blobs = blobIOTestUtility.uploadSourceFiles(5, MiB / 10);

    copier.setBlobSourcePrefix("");

    BlobContainerCopySyncPoller poller = copier.beginCopyOperation();

    poller.waitForCompletion();

    blobs.forEach(
        b ->
            assertThat(
                this.blobIOTestUtility
                    .getDestinationBlobContainerClient()
                    .getBlobClient(b)
                    .exists(),
                is(true)));
  }

  @Test
  public void testCopySingleBLobWithSignedURl_FileIsCopied() {
    String blobName = blobIOTestUtility.uploadSourceFiles(1, MiB / 10).stream().findFirst().get();

    BlobContainerClientFactory sourceFactory = createSourceClientFactoryWithSharedKey();

    BlobContainerCopier copier =
        new BlobContainerCopier(blobIOTestUtility.createDestinationClientFactory());
    copier.setSourceBlobUrl(sourceFactory.createReadOnlySASUrlForBlob(blobName));

    copier.beginCopyOperation().waitForCompletion();

    assertThat(
        this.blobIOTestUtility.getDestinationBlobContainerClient().getBlobClient(blobName).exists(),
        is(true));
  }

  @Test
  public void testCopySingleBLobWithSignedURlAndDestinationName_FileCopiedWithDestinationName() {
    String blobName = blobIOTestUtility.uploadSourceFiles(1, MiB / 10).stream().findFirst().get();
    String destinationBlobName = "myBlob";
    BlobContainerClientFactory sourceFactory = createSourceClientFactoryWithSharedKey();

    BlobContainerCopier copier =
        new BlobContainerCopier(blobIOTestUtility.createDestinationClientFactory());
    copier.setSourceBlobUrl(sourceFactory.createReadOnlySASUrlForBlob(blobName));
    copier.setDestinationBlobName(destinationBlobName);
    copier.beginCopyOperation().waitForCompletion();

    assertThat(
        this.blobIOTestUtility
            .getDestinationBlobContainerClient()
            .getBlobClient(destinationBlobName)
            .exists(),
        is(true));
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

  private BlobContainerCopier createBlobCopierWithSharedKeyInSource(Duration pollingInterval) {

    BlobContainerCopier copier =
        new BlobContainerCopier(this.blobIOTestUtility.createDestinationClientFactory());
    copier.setPollingInterval(pollingInterval);

    copier.setSourceClientFactory(createSourceClientFactoryWithSharedKey());

    return copier;
  }

  private BlobContainerClientFactory createSourceClientFactoryWithSharedKey() {
    String sourceContainer =
        this.blobIOTestUtility.getSourceBlobContainerClient().getBlobContainerName();
    return new BlobContainerClientFactory(
        this.connectedTestConfiguration.getSourceStorageAccountName(),
        getSourceStorageAccountPrimarySharedKey(),
        sourceContainer);
  }

  private BlobContainerCopier createBlobCopierWithTokenCredsInSource(Duration pollingInterval) {

    BlobContainerCopier copier =
        new BlobContainerCopier(this.blobIOTestUtility.createDestinationClientFactory());
    copier.setPollingInterval(pollingInterval);
    String sourceContainer =
        this.blobIOTestUtility.getSourceBlobContainerClient().getBlobContainerName();
    copier.setSourceClientFactory(
        new BlobContainerClientFactory(
            this.connectedTestConfiguration.getSourceStorageAccountName(),
            this.azureResourceConfiguration.getAppToken(),
            sourceContainer));

    return copier;
  }

  private BlobContainerCopier createBlobCopierWithSasCredsInSource(Duration pollingInterval) {

    BlobContainerCopier copier =
        new BlobContainerCopier(this.blobIOTestUtility.createDestinationClientFactory());
    copier.setPollingInterval(pollingInterval);
    copier.setSourceClientFactory(createSourceClientFactoryWithSasCreds());

    return copier;
  }

  private BlobContainerClientFactory createSourceClientFactoryWithSasCreds() {
    return new BlobContainerClientFactory(
        this.blobIOTestUtility.generateSourceContainerUrlWithSasReadAndListPermissions(
            getSourceStorageAccountPrimarySharedKey()));
  }
}

package bio.terra.service.filedata.azure.util;

import static bio.terra.service.resourcemanagement.AzureDataLocationSelector.armUniqueString;

import bio.terra.service.filedata.BlobIOTestUtility;
import bio.terra.service.resourcemanagement.exception.AzureResourceException;
import com.azure.core.credential.TokenCredential;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.policy.RequestRetryOptions;
import com.azure.storage.common.sas.SasProtocol;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.Locale;
import java.util.Optional;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * Test component that facilitates the creation and deletion of data in Azure storage for testing
 * copy operations.
 */
@SuppressFBWarnings(
    value = "DMI_RANDOM_USED_ONLY_ONCE",
    justification = "False positive introduced in 4.2.3, fixed in 4.4.2")
public class AzureBlobIOTestUtility implements BlobIOTestUtility {
  private final BlobContainerClient sourceBlobContainerClient;

  public BlobContainerClient getSourceBlobContainerClient() {
    return sourceBlobContainerClient;
  }

  public BlobContainerClient getDestinationBlobContainerClient() {
    return destinationBlobContainerClient.orElseThrow();
  }

  private final Optional<BlobContainerClient> destinationBlobContainerClient;
  private final String destinationContainerName;
  private static final String STORAGE_ENDPOINT_PATTERN = "https://%s.blob.core.windows.net/%s";
  private final TokenCredential tokenCredential;
  private final RequestRetryOptions retryOptions;

  public AzureBlobIOTestUtility(
      TokenCredential tokenCredential,
      String sourceAccountName,
      String destinationAccountName,
      RequestRetryOptions retryOptions) {
    String sourceContainerName = generateNewTempContainerName();
    destinationContainerName = generateNewTempContainerName();
    sourceBlobContainerClient =
        createBlobContainerClient(tokenCredential, sourceContainerName, sourceAccountName);
    sourceBlobContainerClient.create();
    destinationBlobContainerClient =
        Optional.ofNullable(destinationAccountName)
            .map(a -> createBlobContainerClient(tokenCredential, destinationContainerName, a));
    destinationBlobContainerClient.ifPresent(BlobContainerClient::create);
    this.tokenCredential = tokenCredential;
    this.retryOptions = retryOptions;
  }

  private BlobContainerClient createBlobContainerClient(
      TokenCredential credential, String containerName, String accountName) {

    return new BlobContainerClientBuilder()
        .credential(credential)
        .endpoint(String.format(Locale.ROOT, STORAGE_ENDPOINT_PATTERN, accountName, containerName))
        .buildClient();
  }

  public String generateSourceContainerUrlWithSasReadAndListPermissions(String accountKey) {
    String sasToken = generateContainerSasTokenWithReadAndListPermissions(accountKey);

    return String.format("%s?%s", getSourceBlobContainerClient().getBlobContainerUrl(), sasToken);
  }

  public String generateContainerSasTokenWithReadAndListPermissions(String accountKey) {
    BlobSasPermission permissions =
        new BlobSasPermission().setReadPermission(true).setListPermission(true);

    OffsetDateTime expiryTime = OffsetDateTime.now().plusDays(1);
    SasProtocol sasProtocol = SasProtocol.HTTPS_ONLY;

    // build the token
    BlobServiceSasSignatureValues sasSignatureValues =
        new BlobServiceSasSignatureValues(expiryTime, permissions).setProtocol(sasProtocol);

    BlobContainerClient blobContainerClient =
        new BlobContainerClientBuilder()
            .credential(
                new StorageSharedKeyCredential(
                    sourceBlobContainerClient.getAccountName(), accountKey))
            .endpoint(sourceBlobContainerClient.getBlobContainerUrl())
            .buildClient();

    return blobContainerClient.generateSas(sasSignatureValues);
  }

  public String generateBlobSasTokenWithReadPermissions(String accountKey, String blobName) {
    BlobSasPermission permissions = new BlobSasPermission().setReadPermission(true);

    OffsetDateTime expiryTime = OffsetDateTime.now().plusDays(1);
    SasProtocol sasProtocol = SasProtocol.HTTPS_ONLY;

    // build the token
    BlobServiceSasSignatureValues sasSignatureValues =
        new BlobServiceSasSignatureValues(expiryTime, permissions).setProtocol(sasProtocol);

    BlobClient blobContainerClient =
        new BlobContainerClientBuilder()
            .credential(
                new StorageSharedKeyCredential(
                    sourceBlobContainerClient.getAccountName(), accountKey))
            .endpoint(sourceBlobContainerClient.getBlobContainerUrl())
            .buildClient()
            .getBlobClient(blobName);

    return blobContainerClient.generateSas(sasSignatureValues);
  }

  private String generateNewTempContainerName() {
    return armUniqueString(RandomStringUtils.random(10), 30);
  }

  @Override
  public String uploadSourceFile(String blobName, long length) {
    sourceBlobContainerClient.getBlobClient(blobName).upload(createInputStream(length), length);
    return blobName;
  }

  @Override
  public String uploadFileWithContents(String blobName, String contents) {
    var bytes = contents.getBytes(StandardCharsets.UTF_8);
    try (var byteStream = new ByteArrayInputStream(bytes)) {
      sourceBlobContainerClient.getBlobClient(blobName).upload(byteStream, bytes.length);
      return String.format("%s/%s", getSourceContainerEndpoint(), blobName);
    } catch (IOException ex) {
      throw new AzureResourceException(String.format("Could not write contents to %s", blobName));
    }
  }

  public String uploadDestinationFile(String blobName, long length) {
    getDestinationBlobContainerClient()
        .getBlobClient(blobName)
        .upload(createInputStream(length), length);
    return blobName;
  }

  private String getSourceContainerEndpoint() {
    return sourceBlobContainerClient.getBlobContainerUrl();
  }

  public String createSourcePath(String sourceFile) {
    return String.format("%s/%s", getSourceContainerEndpoint(), sourceFile);
  }

  public String createSourceSignedPath(String sourceFile, String key) {
    return String.format(
        "%s?%s",
        createSourcePath(sourceFile), generateBlobSasTokenWithReadPermissions(key, sourceFile));
  }

  @Override
  public void teardown() {
    destinationBlobContainerClient.ifPresent(BlobContainerClient::delete);
    sourceBlobContainerClient.delete();
  }

  public BlobContainerClientFactory createDestinationClientFactory() {
    return new BlobContainerClientFactory(
        getDestinationBlobContainerClient().getAccountName(),
        tokenCredential,
        destinationContainerName,
        retryOptions);
  }

  public BlobSasTokenOptions createReadOnlyTokenOptions() {
    return new BlobSasTokenOptions(
        Duration.ofHours(1),
        new BlobSasPermission().setReadPermission(true),
        AzureBlobIOTestUtility.class.getName());
  }

  public String getSourceStorageAccountPrimarySharedKey(
      AzureResourceManager client, String resourceGroup, String accountName) {

    return client
        .storageAccounts()
        .getByResourceGroup(resourceGroup, accountName)
        .getKeys()
        .iterator()
        .next()
        .value();
  }
}

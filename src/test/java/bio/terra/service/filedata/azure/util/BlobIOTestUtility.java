package bio.terra.service.filedata.azure.util;

import static bio.terra.service.resourcemanagement.AzureDataLocationSelector.armUniqueString;

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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang3.RandomStringUtils;

/**
 * Test component that facilitates the creation and deletion of data in Azure storage for testing
 * copy operations.
 */
public class BlobIOTestUtility {
  public static final long MIB = 1024 * 1024;
  private static final String SOURCE_BLOB_NAME = "myTestBlob";
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

  public BlobIOTestUtility(
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

  public List<String> uploadSourceFiles(int numOfFiles, long length) {
    return Stream.iterate(0, n -> n + 1)
        .limit(numOfFiles)
        .map(
            i ->
                uploadSourceFile(
                    String.format("%s/%s%s", i, SOURCE_BLOB_NAME, UUID.randomUUID()), length))
        .collect(Collectors.toList());
  }

  public String uploadSourceFile(String blobName, long length) {
    sourceBlobContainerClient.getBlobClient(blobName).upload(createInputStream(length), length);
    return blobName;
  }

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

  private InputStream createInputStream(long length) {
    return new InputStream() {
      private long dataProduced;
      private final SecureRandom rand = new SecureRandom();

      @Override
      public int read() {
        if (dataProduced == length) {
          return -1;
        }
        dataProduced++;
        return rand.nextInt(100 - 65) + 65; // starting at "A"
      }
    };
  }

  public void deleteContainers() {
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
        BlobIOTestUtility.class.getName());
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

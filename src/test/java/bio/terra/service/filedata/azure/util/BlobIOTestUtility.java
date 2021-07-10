package bio.terra.service.filedata.azure.util;

import static bio.terra.service.resourcemanagement.AzureDataLocationSelector.armUniqueString;

import com.azure.core.credential.TokenCredential;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobContainerClientBuilder;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.sas.SasProtocol;
import java.io.IOException;
import java.io.InputStream;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.lang.RandomStringUtils;

/**
 * Test component that facilitates the creation and deletion of data in Azure storage for testing
 * copy operations.
 */
public class BlobIOTestUtility {
  private static final String SOURCE_BLOB_NAME = "myTestBlob";
  static final long MiB = 1024 * 1024;
  private final BlobContainerClient sourceBlobContainerClient;

  public BlobContainerClient getSourceBlobContainerClient() {
    return sourceBlobContainerClient;
  }

  public BlobContainerClient getDestinationBlobContainerClient() {
    return destinationBlobContainerClient;
  }

  private final BlobContainerClient destinationBlobContainerClient;
  private String sourceContainerName;
  private String destinationContainerName;
  private static final String STORAGE_ENDPOINT_PATTERN = "https://%s.blob.core.windows.net/%s";
  private final TokenCredential tokenCredential;

  public BlobIOTestUtility(
      TokenCredential tokenCredential, String sourceAccountName, String destinationAccountName) {
    this.sourceContainerName = generateNewTempContainerName();
    this.destinationContainerName = generateNewTempContainerName();
    this.sourceBlobContainerClient =
        createBlobContainerClient(tokenCredential, this.sourceContainerName, sourceAccountName);
    this.destinationBlobContainerClient =
        createBlobContainerClient(
            tokenCredential, this.destinationContainerName, destinationAccountName);
    this.sourceBlobContainerClient.create();
    this.destinationBlobContainerClient.create();
    this.tokenCredential = tokenCredential;
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

    return String.format(
        "%s?%s", this.getSourceBlobContainerClient().getBlobContainerUrl(), sasToken);
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
                    this.sourceBlobContainerClient.getAccountName(), accountKey))
            .endpoint(this.sourceBlobContainerClient.getBlobContainerUrl())
            .buildClient();

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
                    String.format("%s/%s%s", i, SOURCE_BLOB_NAME, UUID.randomUUID().toString()),
                    length))
        .collect(Collectors.toList());
  }

  public String uploadSourceFile(String blobName, long length) {
    sourceBlobContainerClient.getBlobClient(blobName).upload(createInputStream(length), length);
    return blobName;
  }

  public String uploadDestinationFile(String blobName, long length) {
    destinationBlobContainerClient
        .getBlobClient(blobName)
        .upload(createInputStream(length), length);
    return blobName;
  }

  public String getSourceContainerEndpoint() {
    return this.sourceBlobContainerClient.getBlobContainerUrl();
  }

  public String getDestinationContainerEndpoint() {
    return this.destinationBlobContainerClient.getBlobContainerUrl();
  }

  private InputStream createInputStream(long length) {
    return new InputStream() {
      private long dataProduced;
      private Random rand = new Random();

      @Override
      public int read() throws IOException {
        if (dataProduced == length) {
          return -1;
        }
        dataProduced++;
        return rand.nextInt(100 - 65) + 65; // starting at "A"
      }
    };
  }

  public void deleteContainers() {
    destinationBlobContainerClient.delete();
    sourceBlobContainerClient.delete();
  }

  public BlobContainerClientFactory createDestinationClientFactory() {
    return new BlobContainerClientFactory(
        this.destinationBlobContainerClient.getAccountName(),
        this.tokenCredential,
        this.destinationContainerName);
  }
}

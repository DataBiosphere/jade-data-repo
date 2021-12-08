package common.utils;

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
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.Locale;
import java.util.UUID;

/**
 * Test component that facilitates the creation and deletion of data in Azure storage for testing
 * copy operations.
 */
public class BlobIOTestUtility {
  private final BlobContainerClient sourceBlobContainerClient;

  public BlobContainerClient getSourceBlobContainerClient() {
    return sourceBlobContainerClient;
  }

  private static final String STORAGE_ENDPOINT_PATTERN = "https://%s.blob.core.windows.net/%s";
  private final TokenCredential tokenCredential;
  private final RequestRetryOptions retryOptions;

  public BlobIOTestUtility(
      TokenCredential tokenCredential,
      String sourceAccountName,
      String destinationAccountName,
      RequestRetryOptions retryOptions) {
    String sourceContainerName = "pt" + UUID.randomUUID().toString();
    sourceBlobContainerClient =
        createBlobContainerClient(tokenCredential, sourceContainerName, sourceAccountName);
    sourceBlobContainerClient.create();
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

  public String uploadFileWithContents(String blobName, String contents) throws Exception {
    var bytes = contents.getBytes(StandardCharsets.UTF_8);
    try (var byteStream = new ByteArrayInputStream(bytes)) {
      sourceBlobContainerClient.getBlobClient(blobName).upload(byteStream, bytes.length);
      return String.format("%s/%s", getSourceContainerEndpoint(), blobName);
    } catch (IOException ex) {
      throw new RuntimeException(String.format("Could not write contents to %s", blobName));
    }
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

  public void deleteContainers() {
    sourceBlobContainerClient.delete();
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

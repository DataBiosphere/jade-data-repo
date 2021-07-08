package bio.terra.service.filedata.azure.util;

import com.azure.core.credential.AzureSasCredential;
import com.azure.core.credential.TokenCredential;
import com.azure.core.http.HttpClient;
import com.azure.storage.blob.*;
import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.blob.sas.BlobSasPermission;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.StorageSharedKeyCredential;
import com.azure.storage.common.sas.SasProtocol;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Locale;
import java.util.Objects;
import org.apache.commons.lang.StringUtils;

/**
 * A class that wraps the SDK container client and facilities the creation of SAS Urls for blobs by
 * abstracting the different strategies that depend on the authentication mechanism.
 */
public class BlobContainerClientFactory {

  public static final int EXPIRATION_DAYS = 7;
  private static final HttpClient httpClient = HttpClient.createDefault();

  private final BlobContainerClient blobContainerClient;

  private final SasTokenGeneratorStrategy sasTokenGeneratorStrategy;
  private final AzureSasCredential blobContainerSASTokenCreds;
  private BlobServiceClient blobServiceClient;
  private UserDelegationKey delegationKey;

  public BlobContainerClientFactory(String accountName, String accountKey, String containerName) {

    if (StringUtils.isBlank(accountName)) {
      throw new IllegalArgumentException("The account name is either null or empty.");
    }

    if (StringUtils.isBlank(accountKey)) {
      throw new IllegalArgumentException("The account key is either null or empty.");
    }

    if (StringUtils.isBlank(containerName)) {
      throw new IllegalArgumentException("The container name is either null or empty.");
    }

    this.blobContainerClient =
        createBlobServiceClientUsingSharedKey(accountName, accountKey)
            .getBlobContainerClient(containerName);
    this.sasTokenGeneratorStrategy = SasTokenGeneratorStrategy.SharedKey;
    this.blobContainerSASTokenCreds = null;
  }

  public BlobContainerClientFactory(
      String accountName, TokenCredential azureCredential, String containerName) {

    if (StringUtils.isBlank(accountName)) {
      throw new IllegalArgumentException("The account name is either null or empty.");
    }

    if (StringUtils.isBlank(containerName)) {
      throw new IllegalArgumentException("The container name is either null or empty.");
    }

    this.blobServiceClient =
        createBlobServiceClientUsingTokenCredentials(
            accountName,
            Objects.requireNonNull(azureCredential, "Azure token credentials are null."));
    this.blobContainerClient = this.blobServiceClient.getBlobContainerClient(containerName);

    this.sasTokenGeneratorStrategy = SasTokenGeneratorStrategy.UserDelegatedKey;
    this.blobContainerSASTokenCreds = null;
  }

  public BlobContainerClientFactory(String containerURLWithSASToken) {

    BlobUrlParts blobUrl = BlobUrlParts.parse(containerURLWithSASToken);

    this.blobContainerSASTokenCreds =
        new AzureSasCredential(blobUrl.getCommonSasQueryParameters().encode());
    this.blobContainerClient =
        new BlobContainerClientBuilder()
            .httpClient(httpClient)
            .endpoint(
                String.format(
                    Locale.ROOT,
                    "https://%s/%s",
                    blobUrl.getHost(),
                    blobUrl.getBlobContainerName()))
            .credential(this.blobContainerSASTokenCreds)
            .buildClient();

    this.sasTokenGeneratorStrategy = SasTokenGeneratorStrategy.ContainerSasToken;
  }

  public BlobContainerClient getBlobContainerClient() {
    return this.blobContainerClient;
  }

  public String createReadOnlySASUrlForBlob(String blobName) {
    if (this.sasTokenGeneratorStrategy.equals(SasTokenGeneratorStrategy.SharedKey)) {
      return createReadOnlySasUrlForBlobUsingClient(blobName);
    }

    if (this.sasTokenGeneratorStrategy.equals(SasTokenGeneratorStrategy.ContainerSasToken)) {
      return createReadOnlySasUrlForBlobUsingContainerSAS(blobName);
    }

    if (this.sasTokenGeneratorStrategy.equals(SasTokenGeneratorStrategy.UserDelegatedKey)) {
      return createReadOnlySasUrlForBlobUsingUserDelegatedSas(blobName);
    }

    throw new RuntimeException("Failed to generate SAS token for blob. Invalid strategy set.");
  }

  private synchronized String createReadOnlySasUrlForBlobUsingUserDelegatedSas(String blobName) {

    if (this.delegationKey == null
        || this.delegationKey.getSignedExpiry().isBefore(OffsetDateTime.now(ZoneOffset.UTC))) {
      OffsetDateTime keyExpiry = OffsetDateTime.now(ZoneOffset.UTC).plusDays(EXPIRATION_DAYS);
      this.delegationKey = this.blobServiceClient.getUserDelegationKey(null, keyExpiry);
    }

    BlobServiceSasSignatureValues sasSignatureValues = createSasSignatureValues();

    BlobClient blobClient = blobContainerClient.getBlobClient(blobName);
    String sasToken = blobClient.generateUserDelegationSas(sasSignatureValues, this.delegationKey);

    return String.format(
        "%s/%s?%s", blobContainerClient.getBlobContainerUrl(), blobClient.getBlobName(), sasToken);
  }

  private String createReadOnlySasUrlForBlobUsingContainerSAS(String blobName) {
    return String.format(
        "%s/%s?%s",
        this.blobContainerClient.getBlobContainerUrl(),
        blobName,
        this.blobContainerSASTokenCreds.getSignature());
  }

  private String createReadOnlySasUrlForBlobUsingClient(String blobName) {

    BlobServiceSasSignatureValues sasSignatureValues = createSasSignatureValues();

    BlobClient blobClient = this.blobContainerClient.getBlobClient(blobName);
    String sasToken = blobClient.generateSas(sasSignatureValues);

    return String.format(
        "%s/%s?%s",
        this.blobContainerClient.getBlobContainerUrl(), blobClient.getBlobName(), sasToken);
  }

  private BlobServiceSasSignatureValues createSasSignatureValues() {

    BlobSasPermission permissions = new BlobSasPermission().setReadPermission(true);

    OffsetDateTime expiryTime = OffsetDateTime.now().plusDays(1);
    SasProtocol sasProtocol = SasProtocol.HTTPS_ONLY;

    // build the token
    return new BlobServiceSasSignatureValues(expiryTime, permissions).setProtocol(sasProtocol);
  }

  private BlobServiceClient createBlobServiceClientUsingSharedKey(
      String accountName, String accountKey) {
    return new BlobServiceClientBuilder()
        .credential(new StorageSharedKeyCredential(accountName, accountKey))
        .httpClient(httpClient)
        .endpoint(String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName))
        .buildClient();
  }

  private BlobServiceClient createBlobServiceClientUsingTokenCredentials(
      String accountName, TokenCredential credentials) {
    return new BlobServiceClientBuilder()
        .credential(credentials)
        .httpClient(httpClient)
        .endpoint(String.format(Locale.ROOT, "https://%s.blob.core.windows.net", accountName))
        .buildClient();
  }

  public enum SasTokenGeneratorStrategy {
    SharedKey,
    ContainerSasToken,
    UserDelegatedKey
  }
}

package bio.terra.service.filedata.azure.util;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import com.azure.storage.common.sas.SasProtocol;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import org.apache.commons.lang3.StringUtils;

/** Base class for factories that generate SAS URLs for blobs using tokens from keys. */
public abstract class KeySasUrlFactory implements BlobSasUrlFactory {
  private final BlobContainerClient blobContainerClient;

  public KeySasUrlFactory(BlobContainerClient blobContainerClient) {
    this.blobContainerClient = blobContainerClient;
  }

  abstract String generateSasToken(
      BlobServiceSasSignatureValues sasSignatureValues, BlobClient blobClient);

  @Override
  public String createSasUrlForBlob(String blobName, BlobSasTokenOptions options) {

    BlobClient blobClient = blobContainerClient.getBlobClient(blobName);
    String sasToken = generateSasToken(createSasSignatureValues(options), blobClient);

    return String.format(
        "%s/%s?%s", blobContainerClient.getBlobContainerUrl(), blobClient.getBlobName(), sasToken);
  }

  private BlobServiceSasSignatureValues createSasSignatureValues(
      BlobSasTokenOptions blobSasTokenOptions) {

    OffsetDateTime expiryTime =
        OffsetDateTime.now(ZoneOffset.UTC).plus(blobSasTokenOptions.getExpiration());
    SasProtocol sasProtocol = SasProtocol.HTTPS_ONLY;

    // build the token
    BlobServiceSasSignatureValues values =
        new BlobServiceSasSignatureValues(expiryTime, blobSasTokenOptions.getSasPermissions())
            .setProtocol(sasProtocol);

    if (StringUtils.isNotBlank(blobSasTokenOptions.getContentDisposition())) {
      values.setContentDisposition(blobSasTokenOptions.getContentDisposition());
    }

    return values;
  }
}

package bio.terra.service.common.azure;

import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.common.sas.CommonSasQueryParameters;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Optional;

public final class AzureUriUtils {

  /**
   * The Azure {@link BlobUrlParts} class improperly handles blob names by URL encoding the blob
   * name. This leads to invalid URLs. This method will return the blob name without over encoding.
   *
   * @return A string representing a usable Blob URL
   */
  public static String getUriFromBlobUrlParts(BlobUrlParts blobUrlParts) {
    String sasToken =
        Optional.ofNullable(blobUrlParts.getCommonSasQueryParameters())
            .map(CommonSasQueryParameters::encode)
            .orElse("");
    return String.format(
        "https://%s.blob.core.windows.net/%s/%s%s",
        blobUrlParts.getAccountName(),
        blobUrlParts.getBlobContainerName(),
        URLDecoder.decode(blobUrlParts.getBlobName(), StandardCharsets.UTF_8),
        sasToken.isEmpty() ? "" : "?" + sasToken);
  }
}

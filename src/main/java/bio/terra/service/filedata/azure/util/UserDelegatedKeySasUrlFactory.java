package bio.terra.service.filedata.azure.util;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobServiceClient;
import com.azure.storage.blob.models.UserDelegationKey;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;
import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

/** Generates a SAS token using a User delegated key. */
public class UserDelegatedKeySasUrlFactory extends KeySasUrlFactory {
  private final BlobServiceClient blobServiceClient;
  private final Duration delegatedKeyExpiration;
  private UserDelegationKey delegationKey;

  UserDelegatedKeySasUrlFactory(
      BlobServiceClient blobServiceClient, String containerName, Duration delegatedKeyExpiration) {
    super(blobServiceClient.getBlobContainerClient(containerName));
    this.delegatedKeyExpiration = delegatedKeyExpiration;
    this.blobServiceClient = blobServiceClient;
  }

  @Override
  String generateSasToken(BlobServiceSasSignatureValues sasSignatureValues, BlobClient blobClient) {
    UserDelegationKey userDelegationKey = getUserDelegationKey();

    return blobClient.generateUserDelegationSas(sasSignatureValues, userDelegationKey);
  }

  private synchronized UserDelegationKey getUserDelegationKey() {
    if (delegationKey == null
        || delegationKey.getSignedExpiry().isBefore(OffsetDateTime.now(ZoneOffset.UTC))) {
      OffsetDateTime keyExpiry = OffsetDateTime.now(ZoneOffset.UTC).plus(delegatedKeyExpiration);
      delegationKey = blobServiceClient.getUserDelegationKey(null, keyExpiry);
    }
    return delegationKey;
  }
}

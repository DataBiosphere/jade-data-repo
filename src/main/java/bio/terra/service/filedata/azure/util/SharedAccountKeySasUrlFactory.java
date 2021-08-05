package bio.terra.service.filedata.azure.util;

import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.sas.BlobServiceSasSignatureValues;

/** Generates a SAS token using the Shared Account Key from a storage account. */
public class SharedAccountKeySasUrlFactory extends KeySasUrlFactory {

  SharedAccountKeySasUrlFactory(BlobContainerClient blobContainerClient) {
    super(blobContainerClient);
  }

  @Override
  String generateSasToken(BlobServiceSasSignatureValues sasSignatureValues, BlobClient blobClient) {
    return blobClient.generateSas(sasSignatureValues);
  }
}

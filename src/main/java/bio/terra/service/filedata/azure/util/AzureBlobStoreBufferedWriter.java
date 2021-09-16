package bio.terra.service.filedata.azure.util;

import com.azure.storage.blob.BlobClientBuilder;
import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

/** Given a url to an azure blob, return a buffered reader for the blob */
public class AzureBlobStoreBufferedWriter extends BufferedWriter {
  public AzureBlobStoreBufferedWriter(String ingestRequestSignURL) {
    super(
        new OutputStreamWriter(
            new BlobClientBuilder()
                .endpoint(ingestRequestSignURL)
                .buildClient()
                .getBlockBlobClient()
                .getBlobOutputStream(),
            StandardCharsets.UTF_8));
  }
}

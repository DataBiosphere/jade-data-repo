package bio.terra.service.filedata.azure.util;

import com.azure.storage.blob.BlobClientBuilder;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

/** Given a url to an azure blob, return a buffered reader for the blob */
public class AzureBlobStoreBufferedReader extends BufferedReader {
  public AzureBlobStoreBufferedReader(String ingestRequestSignURL) {
    super(
        new InputStreamReader(
            new BlobClientBuilder().endpoint(ingestRequestSignURL).buildClient().openInputStream(),
            StandardCharsets.UTF_8));
  }
}

package bio.terra.service.filedata.azure.util;

import bio.terra.service.filedata.google.gcs.GcsPdao;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.google.cloud.storage.Storage;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;

/** Given a gs path and a storage object, return a buffered reader for the blob */
public class AzureBlobStoreBufferedReader extends BufferedReader {
  public AzureBlobStoreBufferedReader(String ingestRequestSignURL) {
    super(new InputStreamReader(new BlobClientBuilder().endpoint(ingestRequestSignURL).buildClient().openInputStream(), StandardCharsets.UTF_8));
  }
}

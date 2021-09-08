package bio.terra.service.filedata.azure.blobstore;

import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import com.azure.storage.blob.BlobUrlParts;
import com.google.cloud.storage.Storage;
import org.springframework.beans.factory.annotation.Autowired;
import java.io.BufferedReader;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** Given a gs path and a storage object, return a buffered reader for the blob */
public class AzureBufferedReader extends BufferedReader {

  @Autowired AzureBlobStorePdao azureBlobStorePdao;

    public AzureBufferedReader(Storage storage, UUID tenantId, String blobStoreUrl) {
      IngestUtils.validateBlobAzureBlobFileURL(blobStoreUrl);
      BlobUrlParts ingestRequestSignUrlBlob =
         azureBlobStorePdao.getOrSignUrlForSourceFactory(blobStoreUrl, tenantId);
      // TODO - figure out how to get from URL to buffered reader
//      super(
//          Channels.newReader(
//              GcsPdao.getBlobFromGsPath(storage, blobStoreUrl, projectId).reader(),
//              StandardCharsets.UTF_8.name()));
    }
  }

package bio.terra.service.filedata.azure.blobstore;

import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.specialized.BlobAsyncClientBase;
import com.azure.storage.blob.specialized.BlobClientBase;
import com.azure.storage.blob.specialized.BlobInputStream;
import com.azure.storage.blob.specialized.SpecializedBlobClientBuilder;
import com.google.cloud.storage.Storage;
import org.springframework.beans.factory.annotation.Autowired;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

/** Given a gs path and a storage object, return a buffered reader for the blob */
public class AzureBufferedReader extends BufferedReader {

  @Autowired AzureBlobStorePdao azureBlobStorePdao;

    public AzureBufferedReader(Storage storage, UUID tenantId, String blobStoreUrl) throws IOException {
      IngestUtils.validateBlobAzureBlobFileURL(blobStoreUrl);
      BlobUrlParts ingestRequestSignUrlBlob =
         azureBlobStorePdao.getOrSignUrlForSourceFactory(blobStoreUrl, tenantId);
//      BlobAsyncClientBase asyncClientBase = new BlobAsyncClientBase();
      SpecializedBlobClientBuilder builder = new SpecializedBlobClientBuilder();
      BlobClientBase clientBase = builder.buildPageBlobClient();
      BlobInputStream stream = clientBase.openInputStream();
      // TODO - figure out how to get from URL to buffered reader
      super(stream);
    }
  }

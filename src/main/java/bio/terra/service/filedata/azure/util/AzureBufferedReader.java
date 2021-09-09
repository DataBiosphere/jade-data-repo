package bio.terra.service.filedata.azure.util;

import bio.terra.model.BulkLoadFileModel;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.exception.BulkLoadControlFileException;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.load.LoadService;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobRange;
import com.azure.storage.blob.models.BlobRequestConditions;
import com.azure.storage.blob.specialized.BlobAsyncClientBase;
import com.azure.storage.blob.specialized.BlobClientBase;
import com.azure.storage.blob.specialized.BlobInputStream;
import com.azure.storage.blob.specialized.SpecializedBlobClientBuilder;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.google.cloud.storage.Storage;
import org.springframework.beans.factory.annotation.Autowired;
import java.io.BufferedReader;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/** Given a gs path and a storage object, return a buffered reader for the blob */
public class AzureBufferedReader  {

  @Autowired
  AzureBlobStorePdao azureBlobStorePdao;
  @Autowired
  LoadService loadService;

    public AzureBufferedReader(Storage storage, UUID tenantId, String blobStoreUrl, int maxBadLines, UUID loadId) {
      IngestUtils.validateBlobAzureBlobFileURL(blobStoreUrl);
      BlobUrlParts ingestRequestSignUrlBlob =
         azureBlobStorePdao.getOrSignUrlForSourceFactory(blobStoreUrl, tenantId);
//      BlobAsyncClientBase asyncClientBase = new BlobAsyncClientBase();
      SpecializedBlobClientBuilder builder = new SpecializedBlobClientBuilder();
      BlobClientBase clientBase = builder.buildPageBlobClient();

      // TODO - figure out how to get from URL to buffered reader
      ObjectMapper objectMapper = new ObjectMapper()
          .registerModule(new Jdk8Module())
          .registerModule(new JavaTimeModule())
          .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
      List<String> errorDetails = new ArrayList<>();

      try (BlobInputStream stream = clientBase.openInputStream()) {
        List<BulkLoadFileModel> fileList = new ArrayList<>();

        byte[] b = new byte[0];
        int offset = 0;
        int length = 2;
        int fileLoadBatchSize = 2;
        for (int numRead = stream.read(b, offset, length); b.length > 0; numRead = stream.read(b, offset, length)) {
          offset += numRead;

          try {
            BulkLoadFileModel loadFile = objectMapper.readValue(b, BulkLoadFileModel.class);
            fileList.add(loadFile);
          } catch (IOException ex) {
            errorDetails.add("Format error at byte " + offset + ": " + ex.getMessage());
            if (errorDetails.size() > maxBadLines) {
              throw new BulkLoadControlFileException(
                  "More than " + maxBadLines + " bad lines in the control file", errorDetails);
            }
          }

          // Keep this check and load out of the inner try; it should only catch objectMapper failures
          if (fileList.size() > fileLoadBatchSize) {
            loadService.populateFiles(loadId, fileList);
            fileList.clear();
          }
        }

        // If there are errors in the load file, don't do the load
        if (errorDetails.size() > 0) {
          throw new BulkLoadControlFileException(
              "There were " + errorDetails.size() + " bad lines in the control file", errorDetails);
        }

        if (fileList.size() > 0) {
          loadService.populateFiles(loadId, fileList);
        }

      } catch (IOException ex) {
        throw new BulkLoadControlFileException("Failure accessing the load control file", ex);
      }
    }
  }

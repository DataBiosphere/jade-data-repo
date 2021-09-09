package bio.terra.service.filedata.azure.util;

import bio.terra.model.BulkLoadFileModel;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.exception.BulkLoadControlFileException;
import bio.terra.service.load.LoadService;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.specialized.BlobInputStream;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class BlobReader {

  @Autowired AzureBlobStorePdao azureBlobStorePdao;
  @Autowired LoadService loadService;

  public BlobReader() {}

  public void readFromBlob(UUID tenantId, String blobStoreUrl, int maxBadLines, UUID loadId) {
    IngestUtils.validateBlobAzureBlobFileURL(blobStoreUrl);
    BlobUrlParts ingestRequestSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForSourceFactory(blobStoreUrl, tenantId);
    // This is currently only used in testing - why is that? Am I missing something?
    BlobClient blobClient =
        new BlobClientBuilder().endpoint(ingestRequestSignUrlBlob.toUrl().toString()).buildClient();

    ObjectMapper objectMapper =
        new ObjectMapper()
            .registerModule(new Jdk8Module())
            .registerModule(new JavaTimeModule())
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);
    List<String> errorDetails = new ArrayList<>();

    try (BlobInputStream stream = blobClient.openInputStream()) {
      List<BulkLoadFileModel> fileList = new ArrayList<>();

      int length = 2;
      byte[] b = new byte[length];
      int offset = 0;
      int fileLoadBatchSize = 2;
      // GCP reads line by line - is this possible?
      for (int numRead = stream.read(b, offset, length);
          b.length > 0;
          numRead = stream.read(b, offset, length)) {
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

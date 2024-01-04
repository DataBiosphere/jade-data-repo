package bio.terra.service.filedata.google.util;

import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.filedata.BlobIOTestUtility;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobSourceOption;
import com.google.cloud.storage.Storage.BlobTargetOption;
import com.google.cloud.storage.StorageOptions;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GcsBlobIOTestUtility implements BlobIOTestUtility {
  private final Logger logger = LoggerFactory.getLogger(GcsBlobIOTestUtility.class);

  private final Storage storage = StorageOptions.getDefaultInstance().getService();

  private final String ingestBucket;
  private final String userProject;

  private final List<String> createdBlobs = new ArrayList<>();

  public GcsBlobIOTestUtility(String ingestBucket, String userProject) {
    this.ingestBucket = ingestBucket;
    this.userProject = userProject;
  }

  @Override
  public String uploadSourceFile(String blobName, long length) {
    try (InputStream stream = createInputStream(length)) {
      return uploadFileWithContents(
          blobName, new String(stream.readAllBytes(), StandardCharsets.UTF_8));
    } catch (IOException e) {
      throw new RuntimeException("Error creating test data", e);
    }
  }

  @Override
  public String uploadFileWithContents(String blobName, String contents) {
    String finalBlobName = "%s%s".formatted(blobName, UUID.randomUUID());
    BlobId testBlob = BlobId.of(ingestBucket, finalBlobName);

    BlobTargetOption[] targetOptions = new BlobTargetOption[0];
    if (userProject != null) {
      targetOptions = new BlobTargetOption[] {BlobTargetOption.userProject(userProject)};
    }
    storage.create(
        BlobInfo.newBuilder(testBlob).build(),
        contents.getBytes(StandardCharsets.UTF_8),
        targetOptions);
    return recordAndReturnBlob(finalBlobName);
  }

  public String getFullyQualifiedBlobName(String blobName) {
    return GcsUriUtils.getGsPathFromBlob(BlobId.of(ingestBucket, blobName));
  }

  @Override
  public void teardown() {
    BlobSourceOption[] sourceOptions = new BlobSourceOption[0];
    if (userProject != null) {
      sourceOptions = new BlobSourceOption[] {BlobSourceOption.userProject(userProject)};
    }
    for (String createdBlob : createdBlobs) {
      try {
        storage.delete(BlobId.of(ingestBucket, createdBlob), sourceOptions);
      } catch (Exception e) {
        logger.warn("Couldn't delete blob %s in bucket %s".formatted(createdBlob, ingestBucket), e);
      }
    }
  }

  private String recordAndReturnBlob(String blobName) {
    createdBlobs.add(blobName);
    return blobName;
  }
}

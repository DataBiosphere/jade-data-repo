package bio.terra.common;

import bio.terra.service.common.gcs.GcsUriUtils;
import com.google.cloud.ServiceOptions;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Blob.BlobSourceOption;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobGetOption;
import com.google.cloud.storage.StorageOptions;
import java.nio.charset.StandardCharsets;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class GcsUtils {

  private static final Logger logger = LoggerFactory.getLogger(GcsUtils.class);

  private final String projectId = ServiceOptions.getDefaultProjectId();
  private final Storage storage = StorageOptions.getDefaultInstance().getService();

  public String uploadTestFile(String ingestBucket, String name, List<String> lines) {
    String path = String.format("gs://%s/%s", ingestBucket, name);
    logger.info("Uploading test file to {}", path);
    BlobInfo blobInfo = BlobInfo.newBuilder(ingestBucket, name).build();
    byte[] content = String.join("\n", lines).getBytes(StandardCharsets.UTF_8);
    storage.create(blobInfo, content, Storage.BlobTargetOption.userProject(projectId));
    return path;
  }

  public void deleteTestFile(String path) {
    logger.info("Removing test file at {}", path);
    storage.delete(GcsUriUtils.parseBlobUri(path));
  }

  public boolean fileExists(String path) {
    logger.info("Checking that file {} exists", path);
    Blob blob = storage.get(GcsUriUtils.parseBlobUri(path), BlobGetOption.userProject(projectId));
    return blob.exists(BlobSourceOption.userProject(projectId));
  }
}

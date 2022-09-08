package bio.terra.common;

import bio.terra.common.configuration.TestConfiguration;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class GcsUtils {

  private static Logger logger = LoggerFactory.getLogger(GcsUtils.class);

  @Autowired private TestConfiguration testConfig;
  @Autowired private GcsPdao gcsPdao;

  private String projectId = StorageOptions.getDefaultProjectId();
  private Storage storage = StorageOptions.getDefaultInstance().getService();

  public String uploadTestFile(String ingestBucket, String name, Stream<String> lines) {
    String path = String.format("gs://%s/%s", ingestBucket, name);
    logger.info("Uploading test file to {}", path);
    gcsPdao.createGcsFile(path, projectId);
    gcsPdao.writeStreamToCloudFile(path, lines, projectId);

    return path;
  }

  public void deleteTestFile(String path) {
    logger.info("Removing test file at {}", path);
    gcsPdao.deleteFileByGspath(path, projectId);
  }

  public boolean fileExists(String path) {
    logger.info("Checking that file {} exists", path);
    Blob blob = storage.get(GcsUriUtils.parseBlobUri(path));
    return blob.exists();
  }
}

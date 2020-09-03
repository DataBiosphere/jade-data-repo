package scripts.uploadscripts;

import com.google.api.client.util.ByteStreams;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import common.utils.FileUtils;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.channels.Channels;
import java.nio.file.Path;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uploader.UploadScript;

public class CompressDirectoryToBucket extends UploadScript {
  private static final Logger logger = LoggerFactory.getLogger(CompressDirectoryToBucket.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public CompressDirectoryToBucket() {}

  protected String bucketPath;

  /**
   * Setter for any parameters required by the upload script. These parameters will be set by the
   * Result Uploader based on the current Upload List, and can be used by the upload script methods.
   *
   * @param parameters list of string parameters supplied by the upload list
   */
  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() < 1) {
      throw new IllegalArgumentException("Must provide bucket path in the parameters list");
    }
    bucketPath = parameters.get(0);
    if (!bucketPath.startsWith("gs://")) { // only handle GCS buckets
      throw new IllegalArgumentException("Bucket path must start with gs://");
    }
  }

  /**
   * Upload the test results saved to the given directory. Results may include Test Runner
   * client-side output and any relevant measurements collected.
   */
  public void uploadResults(File outputDirectory) throws Exception {
    // archive file path will be: outputDirectory parent directory + outputDirectory name + .tar.gz
    Path outputDirectoryParent = outputDirectory.toPath().getParent();
    if (outputDirectoryParent == null) {
      throw new IllegalArgumentException(
          "Parent directory of the directory to compress is null: "
              + outputDirectory.getAbsolutePath());
    }
    String archiveFileName = outputDirectory.getName() + ".tar.gz";
    Path archiveFile = outputDirectoryParent.resolve(archiveFileName);

    // create the archive file locally
    FileUtils.compressDirectory(outputDirectory.toPath(), archiveFile);
    logger.info("Compressed directory written locally: {}", archiveFile.toAbsolutePath());

    // upload the archive file to a bucket
    Storage storage = StorageOptions.getDefaultInstance().getService();
    BlobInfo blobInfo =
        BlobInfo.newBuilder(bucketPath.replace("gs://", ""), archiveFileName)
            .setContentType("application/gzip")
            .build();
    try (WriteChannel writer = storage.writer(blobInfo)) {
      try (InputStream inputStream = new FileInputStream(archiveFile.toFile())) {
        ByteStreams.copy(inputStream, Channels.newOutputStream(writer));
      }
    }
    logger.info("Compressed directory written to bucket: {}", blobInfo.getName());
  }
}

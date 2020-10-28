package uploader;

import java.nio.file.Path;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.ServiceAccountSpecification;

public abstract class UploadScript {
  private static final Logger logger = LoggerFactory.getLogger(UploadScript.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public UploadScript() {}

  /**
   * Setter for any parameters required by the upload script. These parameters will be set by the
   * Result Uploader based on the current Upload List, and can be used by the upload script methods.
   *
   * @param parameters list of string parameters supplied by the upload list
   */
  public void setParameters(List<String> parameters) throws Exception {}

  /**
   * Upload the test results saved to the given directory. Results may include Test Runner
   * client-side output and any relevant measurements collected.
   *
   * @param outputDirectory the output directory where the test results are saved
   * @param uploaderServiceAccount the service account to use for the upload
   */
  public void uploadResults(
      Path outputDirectory, ServiceAccountSpecification uploaderServiceAccount) throws Exception {
    throw new UnsupportedOperationException("uploadResults must be overridden by sub-classes");
  }
}

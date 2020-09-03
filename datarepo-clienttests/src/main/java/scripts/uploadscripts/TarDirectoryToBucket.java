package scripts.uploadscripts;

import java.io.File;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uploader.UploadScript;

public class TarDirectoryToBucket extends UploadScript {
  private static final Logger logger = LoggerFactory.getLogger(TarDirectoryToBucket.class);

  /** Public constructor so that this class can be instantiated via reflection. */
  public TarDirectoryToBucket() {}

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
  }

  /**
   * Upload the test results saved to the given directory. Results may include Test Runner
   * client-side output and any relevant measurements collected.
   */
  public void uploadResults(File outputDirectory) throws Exception {
    throw new RuntimeException("mariko: " + outputDirectory.getAbsolutePath());
  }
}

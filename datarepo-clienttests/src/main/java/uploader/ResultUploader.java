package uploader;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.TestRunner;
import uploader.config.UploadList;
import uploader.config.UploadScriptSpecification;

public class ResultUploader {
  private static final Logger logger = LoggerFactory.getLogger(ResultUploader.class);

  private UploadList uploadList;
  private Path outputDirectory;

  protected ResultUploader(UploadList uploadList, Path outputDirectory) {
    this.uploadList = uploadList;
    this.outputDirectory = outputDirectory;
  }

  protected void executeUploadList() throws Exception {
    // loop through the upload script specifications
    for (UploadScriptSpecification specification : uploadList.uploadScripts) {
      // setup an instance of each upload script class
      UploadScript script = specification.scriptClassInstance();
      script.setParameters(specification.parameters);

      // upload the results somewhere
      logger.info("Executing measurement collection script: {}", specification.description);
      script.uploadResults(outputDirectory, uploadList.uploaderServiceAccount);
    }
  }

  public static void uploadResults(String uploadListFileName, String outputDirName)
      throws Exception {
    // read in upload list and validate it
    UploadList uploadList = UploadList.fromJSONFile(uploadListFileName);
    uploadList.validate();

    // build a list of output directories to upload results from
    List<Path> testRunOutputDirectories =
        TestRunner.getTestRunOutputDirectories(Paths.get(outputDirName));

    // loop through each test run output directory, uploading the results separately for each one
    for (int ctr = 0; ctr < testRunOutputDirectories.size(); ctr++) {
      Path testRunOutputDirectory = testRunOutputDirectories.get(ctr);
      logger.info("==== UPLOADING RESULTS FROM TEST CONFIGURATION ({}) ====", ctr);

      // get an instance of an uploader and tell it to execute the upload list
      ResultUploader uploader = new ResultUploader(uploadList, testRunOutputDirectory);
      try {
        uploader.executeUploadList();
      } catch (Exception uplEx) {
        logger.error("Result Uploader threw an exception", uplEx);
      }
    }
  }
}

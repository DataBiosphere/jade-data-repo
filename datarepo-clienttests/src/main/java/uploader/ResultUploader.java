package uploader;

import common.CommandCLI;
import java.io.File;
import java.io.FileNotFoundException;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uploader.config.UploadList;
import uploader.config.UploadScriptSpecification;

public class ResultUploader {
  private static final Logger logger = LoggerFactory.getLogger(ResultUploader.class);

  UploadList uploadList;

  File outputDirectory;

  ResultUploader(UploadList uploadList, File outputDirectory) {
    this.uploadList = uploadList;

    this.outputDirectory = outputDirectory;
  }

  public void executeUploadList() throws Exception {
    // loop through the upload script specifications
    for (UploadScriptSpecification specification : uploadList.uploadScripts) {
      // setup an instance of each upload script class
      UploadScript script = specification.scriptClassInstance();
      script.setParameters(specification.parameters);

      // upload the results somewhere
      logger.info("Executing measurement collection script: {}", specification.description);
      script.uploadResults(outputDirectory);
    }
  }

  public static void uploadResults(String uploadListFileName, String outputDirName)
      throws Exception {
    // read in upload list and validate it
    UploadList uploadList = UploadList.fromJSONFile(uploadListFileName);
    uploadList.validate();

    // get a reference to the output directory
    Path outputDirectory = Paths.get(outputDirName);
    File outputDirectoryFile = outputDirectory.toFile();
    if (!outputDirectoryFile.exists()) {
      throw new FileNotFoundException(
          "Output directory not found: " + outputDirectoryFile.getAbsolutePath());
    }

    // get an instance of an uploader and tell it to execute the upload list
    ResultUploader uploader = new ResultUploader(uploadList, outputDirectoryFile);
    try {
      uploader.executeUploadList();
    } catch (Exception uplEx) {
      logger.error("Result Uploader threw an exception", uplEx);
    }
  }

  public static void main(String[] args) throws Exception {
    CommandCLI.uploadResultsMain(args);
  }
}

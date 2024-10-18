package runner.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import common.utils.FileUtils;
import java.io.File;
import java.io.InputStream;

public class ServiceAccountSpecification implements SpecificationInterface {
  public String name;
  public String jsonKeyCredFilePath;

  public File jsonKeyFile;

  public static final String resourceDirectory = "serviceaccounts";
  public static final String keyCredFilePathEnvironmentVarName =
      "GOOGLE_APPLICATION_CREDENTIALS";

  ServiceAccountSpecification() {}

  /**
   * Read an instance of this class in from a JSON-formatted file. This method expects that the file
   * name exists in the directory specified by {@link #resourceDirectory}
   *
   * @param resourceFileName file name
   * @return an instance of this class
   */
  public static ServiceAccountSpecification fromJSONFile(String resourceFileName) throws Exception {
    // use Jackson to map the stream contents to a TestConfiguration object
    ObjectMapper objectMapper = new ObjectMapper();

    InputStream inputStream =
        FileUtils.getResourceFileHandle(resourceDirectory + "/" + resourceFileName);
    ServiceAccountSpecification serviceAccount =
        objectMapper.readValue(inputStream, ServiceAccountSpecification.class);

    String credFilePathEnvVarOverride = readCredFilePathEnvironmentVariable();
    if (credFilePathEnvVarOverride != null) {
      serviceAccount.jsonKeyCredFilePath = credFilePathEnvVarOverride;
    }

    return serviceAccount;
  }

  protected static String readCredFilePathEnvironmentVariable() {
    // look for a full file path defined for the service account credentials
    //   1. environment variable
    //   2. service account jsonKeyCredFilePath property
    String keyCredFilePathEnvironmentVarValue = System.getenv(keyCredFilePathEnvironmentVarName);
    return keyCredFilePathEnvironmentVarValue;
  }

  /**
   * Validate the service account specification read in from the JSON file. None of the properties
   * should be null.
   */
  public void validate() {
    if (name == null || name.equals("")) {
      throw new IllegalArgumentException("Service account name cannot be empty");
    } else if (jsonKeyCredFilePath == null || jsonKeyCredFilePath.equals("")) {
      throw new IllegalArgumentException("JSON key file path cannot be empty");
    }

    jsonKeyFile = new File(jsonKeyCredFilePath);
    if (!jsonKeyFile.exists()) {
      throw new IllegalArgumentException(
          "JSON key file does not exist: (filePath)" + jsonKeyCredFilePath);
    }
  }
}

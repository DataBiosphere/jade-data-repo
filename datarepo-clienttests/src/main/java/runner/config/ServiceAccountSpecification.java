package runner.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import common.utils.FileUtils;
import java.io.File;
import java.io.InputStream;

public class ServiceAccountSpecification implements SpecificationInterface {
  public String name;
  public String jsonKeyFilePath;

  public File jsonKeyFile;

  public static final String resourceDirectory = "serviceaccounts";

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
    return objectMapper.readValue(inputStream, ServiceAccountSpecification.class);
  }

  /**
   * Validate the service account specification read in from the JSON file. None of the properties
   * should be null.
   */
  public void validate() {
    if (name == null || name.equals("")) {
      throw new IllegalArgumentException("Service account name cannot be empty");
    } else if (jsonKeyFilePath == null || jsonKeyFilePath.equals("")) {
      throw new IllegalArgumentException("JSON key file path cannot be empty");
    }

    jsonKeyFile = new File(jsonKeyFilePath);
    logger.info("file: {}", jsonKeyFile.getPath());
    if (!jsonKeyFile.exists()) {
      throw new IllegalArgumentException("JSON key file does not exist: " + jsonKeyFilePath);
    }
  }
}

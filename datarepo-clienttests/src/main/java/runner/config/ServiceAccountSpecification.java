package runner.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.File;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.FileUtils;

public class ServiceAccountSpecification implements SpecificationInterface {
  private static final Logger LOG = LoggerFactory.getLogger(ServiceAccountSpecification.class);

  public String name;
  public String serviceAccountEmail;
  public String jsonKeyFilePath;
  public String pemFilePath;

  public File jsonKeyFile;
  public File pemFile;

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
        FileUtils.getJSONFileHandle(resourceDirectory + "/" + resourceFileName);
    return objectMapper.readValue(inputStream, ServiceAccountSpecification.class);
  }

  /**
   * Validate the service account specification read in from the JSON file. None of the properties
   * should be null.
   */
  public void validate() {
    if (serviceAccountEmail == null || serviceAccountEmail.equals("")) {
      throw new IllegalArgumentException("Service account email cannot be empty");
    } else if (jsonKeyFilePath == null || jsonKeyFilePath.equals("")) {
      throw new IllegalArgumentException("JSON key file path cannot be empty");
    } else if (pemFilePath == null || pemFilePath.equals("")) {
      throw new IllegalArgumentException("PEM file path cannot be empty");
    }

    jsonKeyFile = new File(jsonKeyFilePath);
    if (!jsonKeyFile.exists()) {
      throw new IllegalArgumentException("JSON key file does not exist: " + jsonKeyFilePath);
    }

    pemFile = new File(pemFilePath);
    if (!pemFile.exists()) {
      throw new IllegalArgumentException("PEM file does not exist: " + pemFilePath);
    }
  }
}

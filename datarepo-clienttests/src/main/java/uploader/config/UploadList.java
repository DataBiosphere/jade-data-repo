package uploader.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import common.utils.FileUtils;
import java.io.InputStream;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.ServiceAccountSpecification;
import runner.config.SpecificationInterface;

public class UploadList implements SpecificationInterface {
  private static final Logger logger = LoggerFactory.getLogger(UploadList.class);

  public String name;
  public String description = "";
  public String uploaderServiceAccountFile;
  public List<UploadScriptSpecification> uploadScripts;

  public ServiceAccountSpecification uploaderServiceAccount;

  public static final String resourceDirectory = "uploadlists";

  UploadList() {}

  /**
   * Read an instance of this class in from a JSON-formatted file. This method expects that the file
   * name exists in the directory specified by {@link #resourceDirectory}
   *
   * @param resourceFileName file name
   * @return an instance of this class
   */
  public static UploadList fromJSONFile(String resourceFileName) throws Exception {
    // use Jackson to map the stream contents to a UploadList object
    ObjectMapper objectMapper = new ObjectMapper();

    // read in the upload list file
    InputStream inputStream =
        FileUtils.getResourceFileHandle(resourceDirectory + "/" + resourceFileName);
    UploadList uploadList = objectMapper.readValue(inputStream, UploadList.class);

    // read in the SA file
    uploadList.uploaderServiceAccount =
        ServiceAccountSpecification.fromJSONFile(uploadList.uploaderServiceAccountFile);

    return uploadList;
  }

  /**
   * Validate the object read in from the JSON file. This method also fills in additional properties
   * of the objects, for example by parsing the string values in the JSON object.
   */
  public void validate() {
    logger.debug("Validating the upload script specifications");
    for (UploadScriptSpecification script : uploadScripts) {
      script.validate();
    }

    uploaderServiceAccount.validate();
  }
}

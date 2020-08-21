package collector.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import common.utils.FileUtils;
import java.io.InputStream;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.SpecificationInterface;

public class MeasurementList implements SpecificationInterface {
  private static final Logger logger = LoggerFactory.getLogger(MeasurementList.class);

  public String name;
  public String description = "";
  public List<MeasurementCollectionScriptSpecification> measurementCollectionScripts;

  public static final String resourceDirectory = "measurementlists";

  MeasurementList() {}

  /**
   * Read an instance of this class in from a JSON-formatted file. This method expects that the file
   * name exists in the directory specified by {@link #resourceDirectory}
   *
   * @param resourceFileName file name
   * @return an instance of this class
   */
  public static MeasurementList fromJSONFile(String resourceFileName) throws Exception {
    // use Jackson to map the stream contents to a MeasurementList object
    ObjectMapper objectMapper = new ObjectMapper();

    // read in the measurement list file
    InputStream inputStream =
        FileUtils.getResourceFileHandle(resourceDirectory + "/" + resourceFileName);
    MeasurementList measurementList = objectMapper.readValue(inputStream, MeasurementList.class);

    return measurementList;
  }

  /**
   * Validate the object read in from the JSON file. This method also fills in additional properties
   * of the objects, for example by parsing the string values in the JSON object.
   */
  public void validate() {
    logger.debug("Validating the measurement collection script specifications");
    for (MeasurementCollectionScriptSpecification script : measurementCollectionScripts) {
      script.validate();
    }
  }
}

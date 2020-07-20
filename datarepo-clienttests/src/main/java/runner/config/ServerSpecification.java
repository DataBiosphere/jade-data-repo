package runner.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.FileUtils;

public class ServerSpecification implements SpecificationInterface {
  private static final Logger LOG = LoggerFactory.getLogger(ServerSpecification.class);

  public String name;
  public String description = "";
  public String uri;
  public String clusterName;
  public String clusterShortName;
  public String region;
  public String project;
  public String namespace;
  public DeploymentScriptSpecification deploymentScript;
  public boolean skipKubernetes = false;
  public boolean skipDeployment = false;

  public static final String resourceDirectory = "servers";

  ServerSpecification() {}

  /**
   * Read an instance of this class in from a JSON-formatted file. This method expects that the file
   * name exists in the directory specified by {@link #resourceDirectory}
   *
   * @param resourceFileName file name
   * @return an instance of this class
   */
  public static ServerSpecification fromJSONFile(String resourceFileName) throws Exception {
    // use Jackson to map the stream contents to a TestConfiguration object
    ObjectMapper objectMapper = new ObjectMapper();

    // read in the server file
    LOG.info("Parsing the server specification file as JSON");
    InputStream inputStream =
        FileUtils.getJSONFileHandle(resourceDirectory + "/" + resourceFileName);
    return objectMapper.readValue(inputStream, ServerSpecification.class);
  }

  /**
   * Validate the server specification read in from the JSON file. None of the properties should be
   * null.
   */
  public void validate() {
    if (uri == null || uri.equals("")) {
      throw new IllegalArgumentException("Server URI cannot be empty");
    }

    if (!skipKubernetes) {
      if (clusterName == null || clusterName.equals("")) {
        throw new IllegalArgumentException("Server cluster name cannot be empty");
      } else if (clusterShortName == null || clusterShortName.equals("")) {
        throw new IllegalArgumentException("Server cluster short name cannot be empty");
      } else if (region == null || region.equals("")) {
        throw new IllegalArgumentException("Server cluster region cannot be empty");
      } else if (project == null || project.equals("")) {
        throw new IllegalArgumentException("Server cluster project cannot be empty");
      }
    }
    if (!skipDeployment) {
      if (deploymentScript == null) {
        throw new IllegalArgumentException("Server deployment script must be defined");
      }
      deploymentScript.validate();
    }
  }
}

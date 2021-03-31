package runner.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import common.utils.FileUtils;
import java.io.InputStream;

public class ServerSpecification implements SpecificationInterface {
  public String name;
  public String description = "";
  public String datarepoUri;
  public String samUri;
  public String samResourceIdForDatarepo;

  // note: the below information specifies the Kubernetes cluster that the Test Runner may
  // manipulate (e.g. kill pods, scale up/down) so in the future, if we want to manipulate the
  // cluster/namespace where SAM or WorkspaceManager is running, that would be the cluster specified
  // by the below fields
  public String clusterName;
  public String clusterShortName;
  public String region;
  public String project;
  public String namespace;
  public String containerName;

  public DeploymentScriptSpecification deploymentScript;
  public String testRunnerServiceAccountFile;
  public ServiceAccountSpecification testRunnerServiceAccount;
  public boolean skipKubernetes = false;
  public boolean skipDeployment = false;
  public boolean dbDropAllOnStart = true;

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
    InputStream inputStream =
        FileUtils.getResourceFileHandle(resourceDirectory + "/" + resourceFileName);
    ServerSpecification server = objectMapper.readValue(inputStream, ServerSpecification.class);

    // read in the service account file
    server.testRunnerServiceAccount =
        ServiceAccountSpecification.fromJSONFile(server.testRunnerServiceAccountFile);

    return server;
  }

  /**
   * Validate the server specification read in from the JSON file. None of the properties should be
   * null.
   */
  public void validate() {
    if (datarepoUri == null || datarepoUri.equals("")) {
      throw new IllegalArgumentException("Data Repo server URI cannot be empty");
    }
    if (samUri == null || samUri.equals("")) {
      throw new IllegalArgumentException("SAM server URI cannot be empty");
    }
    if (samResourceIdForDatarepo == null || samResourceIdForDatarepo.equals("")) {
      throw new IllegalArgumentException("SAM resource id for Data Repo cannot be empty");
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
      } else if (containerName == null || containerName.equals("")) {
        throw new IllegalArgumentException("Server cluster container name cannot be empty");
      }
    }
    if (!skipDeployment) {
      if (deploymentScript == null) {
        throw new IllegalArgumentException("Server deployment script must be defined");
      }
      deploymentScript.validate();
    }

    testRunnerServiceAccount.validate();
  }
}

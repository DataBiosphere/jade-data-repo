package runner.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

  public void display() {
    LOG.info("Server: {}", name);
    LOG.info("  description: {}", description);
    LOG.info("  uri: {}", uri);
    LOG.info("  clusterName: {}", clusterName);
    LOG.info("  clusterShortName: {}", clusterShortName);
    LOG.info("  region: {}", region);
    LOG.info("  project: {}", project);
    LOG.info("  namespace: {}", namespace);
    LOG.info("  skipKubernetes: {}", skipKubernetes);
    LOG.info("  skipDeployment: {}", skipDeployment);

    if (!skipDeployment) {
      deploymentScript.display();
    }
  }
}

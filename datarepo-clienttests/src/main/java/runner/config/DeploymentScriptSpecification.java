package runner.config;

import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.DeploymentScript;

public class DeploymentScriptSpecification implements SpecificationInterface {
  private static final Logger LOG = LoggerFactory.getLogger(DeploymentScriptSpecification.class);

  public String name = "";
  public List<String> parameters = new ArrayList<>();

  public Class<? extends DeploymentScript> scriptClass;

  public static final String scriptsPackage = "deploymentscripts";

  DeploymentScriptSpecification() {}

  /**
   * Validate the deployment script specification read in from the JSON file. The name is converted
   * into a Java class reference
   */
  public void validate() {
    try {
      Class<?> scriptClassGeneric = Class.forName(scriptsPackage + "." + name);
      scriptClass = (Class<? extends DeploymentScript>) scriptClassGeneric;
    } catch (ClassNotFoundException | ClassCastException classEx) {
      throw new IllegalArgumentException("Deployment script class not found: " + name, classEx);
    }
  }

  public void display() {
    LOG.info("Deployment Script: {}", name);
    LOG.info("  name: {}", name);

    String parametersStr = (parameters == null) ? "" : String.join(",", parameters);
    LOG.info("  parameters: {}", parametersStr);
  }
}

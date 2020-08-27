package runner.config;

import java.util.ArrayList;
import java.util.List;
import runner.DeploymentScript;

public class DeploymentScriptSpecification implements SpecificationInterface {
  public String name = "";
  public List<String> parameters = new ArrayList<>();

  public Class<? extends DeploymentScript> scriptClass;

  public static final String scriptsPackage = "scripts.deploymentscripts";

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
}

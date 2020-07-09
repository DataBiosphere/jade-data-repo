package runner.config;

public class ServerSpecification implements SpecificationInterface {
  public String name;
  public String uri;
  public String clusterName;
  public String clusterShortName;
  public String region;
  public String project;
  public String namespace;
  public String helmApiDeploymentFilePath;

  public static final String resourceDirectory = "servers";

  public ServerSpecification() {}

  /**
   * Validate the server specification read in from the JSON file. None of the properties should be
   * null.
   */
  public void validate() {
    if (uri == null || uri.equals("")) {
      throw new IllegalArgumentException("Server URI cannot be empty");
    } else if (clusterName == null || clusterName.equals("")) {
      throw new IllegalArgumentException("Server cluster name cannot be empty");
    } else if (clusterShortName == null || clusterShortName.equals("")) {
      throw new IllegalArgumentException("Server cluster short name cannot be empty");
    } else if (region == null || region.equals("")) {
      throw new IllegalArgumentException("Server cluster region cannot be empty");
    } else if (project == null || project.equals("")) {
      throw new IllegalArgumentException("Server cluster project cannot be empty");
    } else if (helmApiDeploymentFilePath == null || helmApiDeploymentFilePath.equals("")) {
      throw new IllegalArgumentException("Server Helm API deployment file path cannot be empty");
    }
  }

  public void display() {
    System.out.println("Server: " + name);
    System.out.println("  uri: " + uri);
    System.out.println("  clusterName: " + clusterName);
    System.out.println("  clusterShortName: " + clusterShortName);
    System.out.println("  region: " + region);
    System.out.println("  project: " + project);
    System.out.println("  namespace: " + namespace);
    System.out.println("  helmApiDeploymentFilePath: " + helmApiDeploymentFilePath);
  }
}

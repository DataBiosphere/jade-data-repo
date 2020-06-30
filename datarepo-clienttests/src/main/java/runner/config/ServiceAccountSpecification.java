package runner.config;

import java.io.File;

public class ServiceAccountSpecification implements SpecificationInterface {
  public String name;
  public String serviceAccountEmail;
  public String jsonKeyFilePath;
  public String pemFilePath;

  public File jsonKeyFile;
  public File pemFile;

  public static final String resourceDirectory = "serviceaccounts";

  public ServiceAccountSpecification() {}

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

  public void display() {
    System.out.println("Service Account: " + name);
    System.out.println("  serviceAccountEmail: " + serviceAccountEmail);
    System.out.println("  jsonKeyFilePath: " + jsonKeyFilePath);
    System.out.println("  pemFilePath: " + pemFilePath);
  }
}

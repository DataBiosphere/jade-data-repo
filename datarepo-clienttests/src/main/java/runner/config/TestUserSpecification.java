package runner.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUserSpecification implements SpecificationInterface {
  private static final Logger LOG = LoggerFactory.getLogger(TestUserSpecification.class);

  public String name;
  public String userEmail;
  public String delegatorServiceAccountFile;

  public ServiceAccountSpecification delegatorServiceAccount;

  public static final String resourceDirectory = "testusers";

  TestUserSpecification() {}

  /**
   * Validate the test user specification read in from the JSON file. None of the properties should
   * be null.
   */
  public void validate() {
    if (userEmail == null || userEmail.equals("")) {
      throw new IllegalArgumentException("User email cannot be empty");
    } else if (delegatorServiceAccountFile == null || delegatorServiceAccountFile.equals("")) {
      throw new IllegalArgumentException("Delegator service account file cannot be empty");
    }

    delegatorServiceAccount.validate();
  }

  public void display() {
    LOG.info("Test User: {}", name);
    LOG.info("  userEmail: {}", userEmail);
    LOG.info("  delegatorServiceAccountFile: {}", delegatorServiceAccountFile);

    delegatorServiceAccount.display();
  }
}

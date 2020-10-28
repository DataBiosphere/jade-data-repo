package runner.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import common.utils.FileUtils;
import java.io.InputStream;

public class TestUserSpecification implements SpecificationInterface {
  public String name;
  public String userEmail;
  public String delegatorServiceAccountFile;

  public ServiceAccountSpecification delegatorServiceAccount;

  public static final String resourceDirectory = "testusers";
  public static final String delegatorSAFileEnvironmentVarName = "TEST_RUNNER_DELEGATOR_SA_FILE";

  TestUserSpecification() {}

  /**
   * Read an instance of this class in from a JSON-formatted file. This method expects that the file
   * name exists in the directory specified by {@link #resourceDirectory}
   *
   * @param resourceFileName file name
   * @return an instance of this class
   */
  public static TestUserSpecification fromJSONFile(String resourceFileName) throws Exception {
    // use Jackson to map the stream contents to a TestConfiguration object
    ObjectMapper objectMapper = new ObjectMapper();

    InputStream inputStream =
        FileUtils.getResourceFileHandle(resourceDirectory + "/" + resourceFileName);
    TestUserSpecification testUser =
        objectMapper.readValue(inputStream, TestUserSpecification.class);

    // Delegator service account should have permission to act on behalf of the defined test user
    // Which SA to use is determined by the following, in order:
    //   if defined: 1. environment variable (TEST_RUNNER_DELEGATOR_SA_FILE) - uniform across test
    // users
    //   else: 2. test user property (delegatorServiceAccountFile) - specific to the test user
    String delegatorSaEnvVarOverride = System.getenv(delegatorSAFileEnvironmentVarName);
    if (delegatorSaEnvVarOverride != null) {
      testUser.delegatorServiceAccountFile = delegatorSaEnvVarOverride;
      testUser.delegatorServiceAccount =
          ServiceAccountSpecification.fromJSONFile(delegatorSaEnvVarOverride);
    } else {
      testUser.delegatorServiceAccount =
          ServiceAccountSpecification.fromJSONFile(testUser.delegatorServiceAccountFile);
    }

    return testUser;
  }

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
}

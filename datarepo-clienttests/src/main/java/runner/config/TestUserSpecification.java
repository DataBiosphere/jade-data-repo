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

    // read in the server file
    String delegatorSaEnvVarOverride = readDelegatorSAFileEnvironmentVariable();
    if (delegatorSaEnvVarOverride != null) {
      testUser.delegatorServiceAccountFile = delegatorSaEnvVarOverride;
    }
    testUser.delegatorServiceAccount =
        ServiceAccountSpecification.fromJSONFile(testUser.delegatorServiceAccountFile);

    return testUser;
  }

  protected static String readDelegatorSAFileEnvironmentVariable() {
    // the server specification is determined by the following, in order:
    //   1. environment variable
    //   2. test suite server property
    //   3. test configuration server property
    String delegatorSAFileEnvironmentVarValue = System.getenv(delegatorSAFileEnvironmentVarName);
    return delegatorSAFileEnvironmentVarValue;
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

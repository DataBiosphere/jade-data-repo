package runner.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.FileUtils;

public class TestConfiguration implements SpecificationInterface {
  private static final Logger logger = LoggerFactory.getLogger(TestConfiguration.class);

  public String name;
  public String description = "";
  public String serverSpecificationFile;
  public String billingAccount;
  public boolean isFunctional = false;
  public List<String> testUserFiles;

  public ServerSpecification server;
  public KubernetesSpecification kubernetes;
  public ApplicationSpecification application;
  public List<TestScriptSpecification> testScripts;
  public List<TestUserSpecification> testUsers = new ArrayList<>();

  public static final String resourceDirectory = "configs";

  TestConfiguration() {}

  /**
   * Read an instance of this class in from a JSON-formatted file. This method expects that the file
   * name exists in the directory specified by {@link #resourceDirectory}
   *
   * @param resourceFileName file name
   * @return an instance of this class
   */
  public static TestConfiguration fromJSONFile(String resourceFileName) throws Exception {
    // use Jackson to map the stream contents to a TestConfiguration object
    ObjectMapper objectMapper = new ObjectMapper();

    // read in the test config file
    InputStream inputStream =
        FileUtils.getJSONFileHandle(resourceDirectory + "/" + resourceFileName);
    TestConfiguration testConfig = objectMapper.readValue(inputStream, TestConfiguration.class);

    // read in the server file
    testConfig.server = ServerSpecification.fromJSONFile(testConfig.serverSpecificationFile);

    // read in the test user files and the nested service account files
    for (String testUserFile : testConfig.testUserFiles) {
      TestUserSpecification testUser = TestUserSpecification.fromJSONFile(testUserFile);
      testConfig.testUsers.add(testUser);
    }

    // instantiate default kubernetes, application specification objects, if null
    if (testConfig.kubernetes == null) {
      logger.debug("Test Configuration: Using default Kubernetes specification");
      testConfig.kubernetes = new KubernetesSpecification();
    }
    if (testConfig.application == null) {
      logger.debug("Test Configuration: Using default application specification");
      testConfig.application = new ApplicationSpecification();
    }

    return testConfig;
  }

  /**
   * Validate the object read in from the JSON file. This method also fills in additional properties
   * of the objects, for example by parsing the string values in the JSON object.
   */
  public void validate() {
    logger.debug("Validating the server, Kubernetes and application specifications");
    server.validate();
    kubernetes.validate();
    application.validate();

    logger.debug("Validating the test script specifications");
    for (TestScriptSpecification testScript : testScripts) {
      testScript.validate();
      if (isFunctional) {
        if (testScript.numberToRunInParallel > 1) {
          throw new IllegalArgumentException(
              "For a functional test script, the number to run in parallel should be 1.");
        }
      }
      if (server.skipKubernetes && testScript.scriptClassInstance().manipulatesKubernetes()) {
        throw new IllegalArgumentException(
            "The Test Script class "
                + name
                + " manipulates Kubernetes, but the server specification has disabled Kubernetes manipulations (see server.skipKubernetes flag).");
      }
    }

    logger.debug("Validating the test user specifications");
    for (TestUserSpecification testUser : testUsers) {
      testUser.validate();
    }
  }
}

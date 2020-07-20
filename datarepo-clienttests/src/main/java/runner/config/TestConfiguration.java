package runner.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.FileUtils;

public class TestConfiguration implements SpecificationInterface {
  private static final Logger LOG = LoggerFactory.getLogger(TestConfiguration.class);

  public String name;
  public String description = "";
  public String serverSpecificationFile;
  public String billingAccount;
  public List<String> testUserFiles;

  public ServerSpecification server;
  public KubernetesSpecification kubernetes;
  public ApplicationSpecification application;
  public List<TestScriptSpecification> testScripts;
  public List<TestUserSpecification> testUsers = new ArrayList<>();

  public static final String resourceDirectory = "configs";

  TestConfiguration() {}

  public static TestConfiguration fromJSONFile(String resourceFileName) throws Exception {
    // use Jackson to map the stream contents to a TestConfiguration object
    ObjectMapper objectMapper = new ObjectMapper();

    // read in the test config file
    LOG.info("Test Configuration: Parsing the test configuration file as JSON");
    InputStream inputStream =
        FileUtils.getJSONFileHandle(resourceDirectory + "/" + resourceFileName);
    TestConfiguration testConfig = objectMapper.readValue(inputStream, TestConfiguration.class);

    // read in the server file
    LOG.info("Test Configuration: Parsing the server specification file as JSON");
    inputStream =
        FileUtils.getJSONFileHandle(
            ServerSpecification.resourceDirectory + "/" + testConfig.serverSpecificationFile);
    testConfig.server = objectMapper.readValue(inputStream, ServerSpecification.class);

    // read in the test user files and the nested service account files
    LOG.info(
        "Test Configuration: Parsing the test user and service account specification files as JSON");
    for (String testUserFile : testConfig.testUserFiles) {
      inputStream =
          FileUtils.getJSONFileHandle(TestUserSpecification.resourceDirectory + "/" + testUserFile);
      TestUserSpecification testUser =
          objectMapper.readValue(inputStream, TestUserSpecification.class);

      inputStream =
          FileUtils.getJSONFileHandle(
              ServiceAccountSpecification.resourceDirectory
                  + "/"
                  + testUser.delegatorServiceAccountFile);
      testUser.delegatorServiceAccount =
          objectMapper.readValue(inputStream, ServiceAccountSpecification.class);
      testConfig.testUsers.add(testUser);
    }

    // instantiate default kubernetes, application specification objects, if null
    if (testConfig.kubernetes == null) {
      LOG.info("Test Configuration: Using default Kubernetes specification");
      testConfig.kubernetes = new KubernetesSpecification();
    }
    if (testConfig.application == null) {
      LOG.info("Test Configuration: Using default application specification");
      testConfig.application = new ApplicationSpecification();
    }

    return testConfig;
  }

  /**
   * Validate the object read in from the JSON file. This method also fills in additional properties
   * of the objects, for example by parsing the string values in the JSON object.
   */
  public void validate() {
    LOG.info(
        "Test Configuration: Validating the server, Kubernetes and application specifications");
    server.validate();
    kubernetes.validate();
    application.validate();

    LOG.info("Test Configuration: Validating the test script specifications");
    for (TestScriptSpecification testScript : testScripts) {
      testScript.validate();
    }

    LOG.info("Test Configuration: Validating the test user specifications");
    for (TestUserSpecification testUser : testUsers) {
      testUser.validate();
    }
  }

  public void display() {
    LOG.info("Test configuration: {}", name);
    LOG.info("  Description: {}", description);

    server.display();
    kubernetes.display();
    application.display();

    LOG.info("  Billing account: {}", billingAccount);

    for (TestScriptSpecification testScript : testScripts) {
      testScript.display();
    }

    for (TestUserSpecification testUser : testUsers) {
      testUser.display();
    }
  }
}

package runner.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import common.utils.FileUtils;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSuite implements SpecificationInterface {
  private static final Logger logger = LoggerFactory.getLogger(TestSuite.class);

  public String name;
  public String description = "";
  public String
      serverSpecificationFile; // overrides the server specification for all test configurations
  public List<String> testConfigurationFiles;

  ServerSpecification server;
  public List<TestConfiguration> testConfigurations = new ArrayList<>();

  public static final String resourceDirectory = "suites";

  TestSuite() {}

  /**
   * Read an instance of this class in from a JSON-formatted file. This method expects that the file
   * name exists in the directory specified by {@link #resourceDirectory}
   *
   * @param resourceFileName file name
   * @return an instance of this class
   */
  public static TestSuite fromJSONFile(String resourceFileName) throws Exception {
    // use Jackson to map the stream contents to a TestConfiguration object
    ObjectMapper objectMapper = new ObjectMapper();

    // read in the test suite file
    logger.info("Parsing the test suite file as JSON");
    InputStream inputStream =
        FileUtils.getResourceFileHandle(resourceDirectory + "/" + resourceFileName);
    TestSuite testSuite = objectMapper.readValue(inputStream, TestSuite.class);

    // read in the server file
    String serverEnvVarOverride = TestConfiguration.readServerEnvironmentVariable();
    if (serverEnvVarOverride != null) {
      testSuite.serverSpecificationFile = serverEnvVarOverride;
    }
    testSuite.server = ServerSpecification.fromJSONFile(testSuite.serverSpecificationFile);

    // read in the test config files
    for (String testConfigurationFile : testSuite.testConfigurationFiles) {
      logger.info("Parsing the test configuration file as JSON");
      TestConfiguration testConfig = TestConfiguration.fromJSONFile(testConfigurationFile);

      // override the server specification defined in each test configuration with the one defined
      // for the test suite
      testConfig.serverSpecificationFile = testSuite.serverSpecificationFile;
      testConfig.server = testSuite.server;

      testSuite.testConfigurations.add(testConfig);
    }

    return testSuite;
  }

  public static TestSuite fromSingleTestConfiguration(TestConfiguration testConfiguration)
      throws Exception {
    TestSuite testSuite = new TestSuite();
    testSuite.name = "GENERATED_SingleTestSuite";
    testSuite.description = "Single Test Configuration: " + testConfiguration.description;
    testSuite.serverSpecificationFile = testConfiguration.serverSpecificationFile;
    testSuite.server = testConfiguration.server;
    testSuite.testConfigurations.add(testConfiguration);
    return testSuite;
  }

  /**
   * Validate the object read in from the JSON file. This method also fills in additional properties
   * of the objects, for example by parsing the string values in the JSON object.
   */
  public void validate() {
    logger.info("Validating the test configurations");
    for (TestConfiguration testConfig : testConfigurations) {
      testConfig.validate();
    }
  }
}

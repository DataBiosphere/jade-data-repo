package runner.config;

import com.fasterxml.jackson.databind.*;
import java.io.*;
import runner.*;
import utils.*;

public class FailureScriptSpecification implements SpecificationInterface {
  // variables defined in failure config json
  public String name;
  public String description;
  public String failureScriptName;
  public int podCount = 0;
  // todo add these other variables
  // public int kubernetesWaitBeforeKillingPod = 30;
  // public String kubernetesWaitBeforeKillingPodUnit= "SECONDS";

  // objects defined here
  private TestScript failureScriptClassInstance;

  // constants
  public static final String failureConfigs = "failureconfigs";
  public static final String testScriptsPackage = "testscripts";

  FailureScriptSpecification() {}

  public TestScript failureScriptClassInstance() {
    return failureScriptClassInstance;
  }

  // todo add method description
  public static FailureScriptSpecification fromJSONFile(String failureConfigFile) throws Exception {
    // use Jackson to map the stream contents to a TestConfiguration object
    ObjectMapper objectMapper = new ObjectMapper();

    // read in the server file
    InputStream inputStream = FileUtils.getJSONFileHandle(failureConfigs + "/" + failureConfigFile);
    return objectMapper.readValue(inputStream, FailureScriptSpecification.class);
  }

  /** Validate the Failure specification read in from the JSON file. */
  public void validate() {
    if (podCount <= 0) {
      // todo
      // then we want to check that the other parameters are set
      // throw new IllegalArgumentException("Number of initial Kubernetes pods must be >= 0");
    }

    try {
      Class<?> scriptClassGeneric = Class.forName(testScriptsPackage + "." + failureScriptName);
      Class<? extends TestScript> scriptClass = (Class<? extends TestScript>) scriptClassGeneric;
      failureScriptClassInstance = scriptClass.newInstance();
    } catch (ClassNotFoundException | ClassCastException classEx) {
      throw new IllegalArgumentException(
          "Test script class not found: " + failureScriptName, classEx);
    } catch (IllegalAccessException | InstantiationException niEx) {
      throw new IllegalArgumentException(
          "Error calling constructor of TestScript class: " + failureScriptName, niEx);
    }
  }
}

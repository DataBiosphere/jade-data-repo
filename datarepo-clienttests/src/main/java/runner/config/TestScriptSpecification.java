package runner.config;

import java.util.List;
import runner.TestScript;

public class TestScriptSpecification implements SpecificationInterface {
  // variables defined in json config
  public String name;
  public int totalNumberToRun = 1;
  public long expectedSecondsForEach;
  public List<String> parameters;

  // objects defined in this class
  private TestScript scriptClassInstance;

  public String description;

  public static final String scriptsPackage = "testscripts";

  TestScriptSpecification() {}

  public TestScript scriptClassInstance() {
    return scriptClassInstance;
  }

  /**
   * Validate the test script specification read in from the JSON file. The time unit string is
   * parsed into a TimeUnit; the name is converted into a Java class reference.
   */
  public void validate() {
    if (totalNumberToRun <= 0) {
      throw new IllegalArgumentException("Total number to run must be >=0.");
    }
    if (expectedSecondsForEach <= 0) {
      throw new IllegalArgumentException("Expected time for each must be >=0.");
    }

    try {
      Class<?> scriptClassGeneric = Class.forName(scriptsPackage + "." + name);
      Class<? extends TestScript> scriptClass = (Class<? extends TestScript>) scriptClassGeneric;
      scriptClassInstance = scriptClass.newInstance();
    } catch (ClassNotFoundException | ClassCastException classEx) {
      throw new IllegalArgumentException("Test script class not found: " + name, classEx);
    } catch (IllegalAccessException | InstantiationException niEx) {
      throw new IllegalArgumentException(
          "Error calling constructor of TestScript class: " + name, niEx);
    }

    // generate a separate description property that also includes any test script parameters
    description = name;
    if (parameters != null) {
      description += ": " + String.join(",", parameters);
    }
  }
}

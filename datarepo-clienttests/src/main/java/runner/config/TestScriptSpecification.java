package runner.config;

import java.util.List;
import java.util.concurrent.TimeUnit;
import runner.TestScript;

public class TestScriptSpecification implements SpecificationInterface {
  // variables defined in json config
  public String name;
  public int totalNumberToRun = 1;
  public int numberToRunInParallel = 1;
  public long expectedTimeForEach;
  public String expectedTimeForEachUnit;
  public List<String> parameters;
  public String failureScriptFile;

  // objects defined in this class
  private TestScript scriptClassInstance;
  private FailureScriptSpecification failureScriptSpecification;
  public TimeUnit expectedTimeForEachUnitObj;
  public String description;

  public static final String scriptsPackage = "testscripts";

  TestScriptSpecification() {}

  public TestScript scriptClassInstance() {
    return scriptClassInstance;
  }

  public FailureScriptSpecification failureScriptSpecification() {
    return failureScriptSpecification;
  }

  /**
   * Validate the test script specification read in from the JSON file. The time unit string is
   * parsed into a TimeUnit; the name is converted into a Java class reference.
   */
  public void validate() {
    if (totalNumberToRun <= 0) {
      throw new IllegalArgumentException("Total number to run must be >=0.");
    }
    if (numberToRunInParallel <= 0) {
      throw new IllegalArgumentException("Number to run in parallel must be >=0.");
    }
    if (expectedTimeForEach <= 0) {
      throw new IllegalArgumentException("Expected time for each must be >=0.");
    }

    expectedTimeForEachUnitObj = TimeUnit.valueOf(expectedTimeForEachUnit);

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

    // todo: Where is the best place to do this sort of check for items that are not required?
    if (failureScriptFile != null && !failureScriptFile.isEmpty()) {
      try {
        // For each test, we can designate a failure script to run alongside the test scripts
        // Convert this failure script from json to the FailureScriptSpecification class
        failureScriptSpecification = FailureScriptSpecification.fromJSONFile(failureScriptFile);
      } catch (Exception ex) {
        logger.debug("Error parsing failure script. Error: {}", ex);
      }
      // since the failure script is added on a per-testscript basis, we can't do the validate check
      // at the
      // TestConfiguration level along with the other validate checks
      failureScriptSpecification.validate();
    }

    // generate a separate description property that also includes any test script parameters
    description = name;
    if (parameters != null) {
      description += ": " + String.join(",", parameters);
    }
  }
}

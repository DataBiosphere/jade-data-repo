package runner.config;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.TestScript;

public class TestScriptSpecification implements SpecificationInterface {
  private static final Logger LOG = LoggerFactory.getLogger(TestScriptSpecification.class);

  public String name;
  public int totalNumberToRun;
  public int numberToRunInParallel;
  public long expectedTimeForEach;
  public String expectedTimeForEachUnit;
  public List<String> parameters;

  public Class<? extends TestScript> scriptClass;
  public TimeUnit expectedTimeForEachUnitObj;
  public String description;

  public static final String scriptsPackage = "testscripts";

  TestScriptSpecification() {}

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
      scriptClass = (Class<? extends TestScript>) scriptClassGeneric;
    } catch (ClassNotFoundException | ClassCastException classEx) {
      throw new IllegalArgumentException("Test script class not found: " + name, classEx);
    }

    // generate a separate description property that also includes any test script parameters
    description = name;
    if (parameters != null) {
      description += ": " + String.join(",", parameters);
    }
  }
}

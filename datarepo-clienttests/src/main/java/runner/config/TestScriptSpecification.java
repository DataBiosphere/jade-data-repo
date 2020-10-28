package runner.config;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import java.util.concurrent.TimeUnit;
import runner.TestScript;

@SuppressFBWarnings(
    value = "UWF_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD",
    justification = "This POJO class is used for easy serialization to JSON using Jackson.")
public class TestScriptSpecification implements SpecificationInterface {
  public String name;
  public int numberOfUserJourneyThreadsToRun = 1;
  public int userJourneyThreadPoolSize = 1;
  public long expectedTimeForEach;
  public String expectedTimeForEachUnit;
  public List<String> parameters;

  private TestScript scriptClassInstance;
  public TimeUnit expectedTimeForEachUnitObj;
  public String description;

  public static final String scriptsPackage = "scripts.testscripts";

  TestScriptSpecification() {}

  public TestScript scriptClassInstance() {
    return scriptClassInstance;
  }

  /**
   * Validate the test script specification read in from the JSON file. The time unit string is
   * parsed into a TimeUnit; the name is converted into a Java class reference.
   */
  public void validate() {
    if (numberOfUserJourneyThreadsToRun <= 0) {
      throw new IllegalArgumentException("Number of user journey threads to run must be >=0.");
    }
    if (userJourneyThreadPoolSize <= 0) {
      throw new IllegalArgumentException("User journey thread pool size must be >=0.");
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

    // generate a separate description property that also includes any test script parameters
    description = name;
    if (parameters != null) {
      description += ": " + String.join(",", parameters);
    }
  }
}

package runner.config;

import com.fasterxml.jackson.databind.*;
import java.util.List;
import runner.TestScript;

public class FailureScriptSpecification implements SpecificationInterface {
  public String description;
  public String failureScriptName;
  public List<String> parameters;

  private TestScript failureScriptClassInstance;

  public static final String testScriptsPackage = "failurescripts";

  FailureScriptSpecification() {}

  public TestScript failureScriptClassInstance() {
    return failureScriptClassInstance;
  }

  public void validate() {
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

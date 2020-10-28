package runner.config;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import runner.DisruptiveScript;

@SuppressFBWarnings(
    value = "UWF_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD",
    justification = "This POJO class is used for easy serialization to JSON using Jackson.")
public class DisruptiveScriptSpecification implements SpecificationInterface {
  public String name;
  public List<String> parameters;

  private DisruptiveScript disruptiveScriptClassInstance;

  public static final String disruptiveScriptsPackage = "scripts.disruptivescripts";

  DisruptiveScriptSpecification() {}

  public DisruptiveScript disruptiveScriptClassInstance() {
    return disruptiveScriptClassInstance;
  }

  public void validate() {
    try {
      Class<?> scriptClassGeneric = Class.forName(disruptiveScriptsPackage + "." + name);
      Class<? extends DisruptiveScript> scriptClass =
          (Class<? extends DisruptiveScript>) scriptClassGeneric;
      disruptiveScriptClassInstance = scriptClass.newInstance();
    } catch (ClassNotFoundException | ClassCastException classEx) {
      throw new IllegalArgumentException("Disruptive script class not found: " + name, classEx);
    } catch (IllegalAccessException | InstantiationException niEx) {
      throw new IllegalArgumentException(
          "Error calling constructor of Disruptive Script class: " + name, niEx);
    }
  }
}

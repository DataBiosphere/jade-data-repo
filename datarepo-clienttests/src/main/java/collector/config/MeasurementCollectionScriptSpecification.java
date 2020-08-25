package collector.config;

import collector.MeasurementCollectionScript;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import runner.config.SpecificationInterface;

@SuppressFBWarnings(
    value = "UWF_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD",
    justification = "This POJO class is used for easy serialization to JSON using Jackson.")
public class MeasurementCollectionScriptSpecification implements SpecificationInterface {
  public String name;
  public String description;
  public List<String> parameters;
  public boolean saveRawDataPoints;

  private MeasurementCollectionScript scriptClassInstance;

  public static final String scriptsPackage = "scripts.measurementcollectionscripts";

  MeasurementCollectionScriptSpecification() {}

  public MeasurementCollectionScript scriptClassInstance() {
    return scriptClassInstance;
  }

  /**
   * Validate the measurement collection script specification read in from the JSON file. The name
   * is converted into a Java class reference.
   */
  public void validate() {
    try {
      Class<?> scriptClassGeneric = Class.forName(scriptsPackage + "." + name);
      Class<? extends MeasurementCollectionScript> scriptClass =
          (Class<? extends MeasurementCollectionScript>) scriptClassGeneric;
      scriptClassInstance = scriptClass.newInstance();
    } catch (ClassNotFoundException | ClassCastException classEx) {
      throw new IllegalArgumentException(
          "Measurement collection script class not found: " + name, classEx);
    } catch (IllegalAccessException | InstantiationException niEx) {
      throw new IllegalArgumentException(
          "Error calling constructor of MeasurementCollectionScript class: " + name, niEx);
    }
  }
}

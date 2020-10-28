package uploader.config;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import runner.config.SpecificationInterface;
import uploader.UploadScript;

@SuppressFBWarnings(
    value = "UWF_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD",
    justification = "This POJO class is used for easy serialization to JSON using Jackson.")
public class UploadScriptSpecification implements SpecificationInterface {
  public String name;
  public String description;
  public List<String> parameters;

  private UploadScript scriptClassInstance;

  public static final String scriptsPackage = "scripts.uploadscripts";

  UploadScriptSpecification() {}

  public UploadScript scriptClassInstance() {
    return scriptClassInstance;
  }

  /**
   * Validate the upload script specification read in from the JSON file. The name is converted into
   * a Java class reference.
   */
  public void validate() {
    try {
      Class<?> scriptClassGeneric = Class.forName(scriptsPackage + "." + name);
      Class<? extends UploadScript> scriptClass =
          (Class<? extends UploadScript>) scriptClassGeneric;
      scriptClassInstance = scriptClass.newInstance();
    } catch (ClassNotFoundException | ClassCastException classEx) {
      throw new IllegalArgumentException("Upload script class not found: " + name, classEx);
    } catch (IllegalAccessException | InstantiationException niEx) {
      throw new IllegalArgumentException(
          "Error calling constructor of UploadScript class: " + name, niEx);
    }
  }
}

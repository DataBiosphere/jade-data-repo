package runner;

import java.util.List;
import runner.config.TestUserSpecification;

public abstract class DisruptiveScript {

  /** Public constructor so that this class can be instantiated via reflection. */
  public DisruptiveScript() {}

  protected boolean manipulatesKubernetes = false;

  /**
   * Getter for the manipulates Kubernetes property of this class. This property may be overridden
   * by script classes that manipulate Kubernetes as part of the disrupt method. The default value
   * of this property is false (i.e. Kubernetes is not manipulated).
   *
   * @return true if Kubernetes is required, false otherwise
   */
  public boolean manipulatesKubernetes() {
    return manipulatesKubernetes;
  }

  /**
   * Setter for any parameters required by the disrupt script. These parameters will be set by the
   * Test Runner based on the current Test Configuration, and can be used by the Disruptive script
   * methods.
   *
   * @param parameters list of string parameters supplied by the test configuration
   */
  public void setParameters(List<String> parameters) throws Exception {}

  /**
   * The test script disrupt method contains the actions we want to perform in order disrupt the
   * test run and profile resiliency
   */
  public void disrupt(List<TestUserSpecification> testUsers) throws Exception {
    throw new UnsupportedOperationException("disrupt method must be overridden by sub-classes");
  }
}

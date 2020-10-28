package runner;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.List;
import runner.config.ServerSpecification;
import runner.config.TestUserSpecification;

@SuppressFBWarnings(
    value = "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD",
    justification =
        "There are no disruptive scripts that currently need a billing account, but a disruptive script should have all the information that a test script has.")
public abstract class DisruptiveScript {

  /** Public constructor so that this class can be instantiated via reflection. */
  public DisruptiveScript() {}

  protected String billingAccount;
  protected ServerSpecification server;
  protected boolean manipulatesKubernetes = false;

  /**
   * Setter for the billing account property of this class. This property will be set by the Test
   * Runner based on the current Test Configuration, and can be accessed by the Disruptive Script
   * methods.
   *
   * @param billingAccount Google billing account id
   */
  public void setBillingAccount(String billingAccount) {
    this.billingAccount = billingAccount;
  }

  /**
   * Setter for the server specification property of this class. This property will be set by the
   * Test Runner based on the current Test Configuration, and can be accessed by the Disruptive
   * Script methods.
   *
   * @param server the specification of the server(s) this test runs against
   */
  public void setServer(ServerSpecification server) {
    this.server = server;
  }

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

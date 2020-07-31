package runner;

import bio.terra.datarepo.client.ApiClient;
import java.util.List;
import java.util.Map;

public abstract class TestScript implements ScriptInterface {

  /** Public constructor so that this class can be instantiated via reflection. */
  public TestScript() {}

  protected String billingAccount;
  protected boolean manipulatesKubernetes = false;

  /**
   * Setter for the billing account property of this class. This property will be set by the Test
   * Runner based on the current Test Configuration, and can be accessed by the Test Script methods.
   *
   * @param billingAccount Google billing account id
   */
  public void setBillingAccount(String billingAccount) {
    this.billingAccount = billingAccount;
  }

  /**
   * Getter for the manipulates Kubernetes property of this class. This property may be overridden
   * by Test Script classes that manipulate Kubernetes as part of the setup, cleanup, or userJourney
   * methods. The default value of this property is false (i.e. Kubernetes is not manipulated).
   *
   * @return true if Kubernetes is required, false otherwise
   */
  public boolean manipulatesKubernetes() {
    return manipulatesKubernetes;
  }

  /**
   * Setter for any parameters required by the test script. These parameters will be set by the Test
   * Runner based on the current Test Configuration, and can be used by the Test script methods.
   *
   * @param parameters list of string parameters supplied by the test configuration
   */
  public void setParameters(List<String> parameters) throws Exception {}

  /**
   * The test script setup contains the API call(s) that we do not want to profile and will not be
   * scaled to run multiple in parallel. setup() is called once at the beginning of the test run.
   */
  public void setup(Map<String, ApiClient> apiClients) throws Exception {}

  /**
   * The test script userJourney contains the API call(s) that we want to profile and it may be
   * scaled to run multiple journeys in parallel.
   */
  public void userJourney(ApiClient apiClient) throws Exception {
    throw new UnsupportedOperationException("userJourney must be overridden by sub-classes");
  }

  /**
   * The test script cleanup contains the API call(s) that we do not want to profile and will not be
   * scaled to run multiple in parallel. cleanup() is called once at the end of the test run.
   */
  public void cleanup(Map<String, ApiClient> apiClients) throws Exception {}

  /**
   * This method runs the test script methods in the expected order: setup, userJourney, cleanup. It
   * is intended for easier debugging (e.g. API calls, building request models) when writing a new
   * script.
   *
   * <p>public static void main(String[] args) throws Exception { // get an instance of the script
   * and the API client TestScript testScript = new TestScript();
   *
   * <p>ApiClient apiClient = Configuration.getDefaultApiClient(); Map<String, ApiClient> apiClients
   * = new HashMap<>(); apiClients.put("default", apiClient);
   *
   * <p>testScript.setup(apiClients); testScript.userJourney(apiClient);
   * testScript.cleanup(apiClients); }
   */
}

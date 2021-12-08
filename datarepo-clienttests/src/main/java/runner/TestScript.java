package runner;

import bio.terra.datarepo.model.CloudPlatform;
import java.util.List;
import java.util.UUID;
import runner.config.ServerSpecification;
import runner.config.TestUserSpecification;

public abstract class TestScript {

  /** Public constructor so that this class can be instantiated via reflection. */
  public TestScript() {}

  protected String billingAccount;
  protected ServerSpecification server;
  protected boolean manipulatesKubernetes = false;
  protected CloudPlatform cloudPlatform;
  protected UUID tenantId;
  protected UUID subscriptionId;
  protected String resourceGroupName;
  protected String applicationDeploymentName;
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
   * Setter for the server specification property of this class. This property will be set by the
   * Test Runner based on the current Test Configuration, and can be accessed by the Test Script
   * methods.
   *
   * @param server the specification of the server(s) this test runs against
   */
  public void setServer(ServerSpecification server) {
    this.server = server;
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

  /** Setter for the cloud platform to use. */
  public void setCloudPlatform(CloudPlatform cloudPlatform) {
    this.cloudPlatform = cloudPlatform;
  }

  public void setTenantId(UUID tenantId) {
    this.tenantId = tenantId;
  }

  public void setSubscriptionId(UUID subscriptionId) {
    this.subscriptionId = subscriptionId;
  }

  public void setResourceGroupName(String resourceGroupName) {
    this.resourceGroupName = resourceGroupName;
  }

  public void setApplicationDeploymentName(String applicationDeploymentName) {
    this.applicationDeploymentName = applicationDeploymentName;
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
  public void setup(List<TestUserSpecification> testUsers) throws Exception {}

  /**
   * The test script userJourney contains the API call(s) that we want to profile and it may be
   * scaled to run multiple journeys in parallel.
   */
  public void userJourney(TestUserSpecification testUser) throws Exception {
    throw new UnsupportedOperationException("userJourney must be overridden by sub-classes");
  }

  /**
   * The test script cleanup contains the API call(s) that we do not want to profile and will not be
   * scaled to run multiple in parallel. cleanup() is called once at the end of the test run.
   */
  public void cleanup(List<TestUserSpecification> testUsers) throws Exception {}
}

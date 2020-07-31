package runner;

public abstract class FailureScript implements ScriptInterface {

  /** Public constructor so that this class can be instantiated via reflection. */
  public FailureScript() {}

  protected int podCount = 1;
  // public long timeBeforeFailure;
  // public String timeBeforeFailureUnit;

  /**
   * Setter for the podCount property
   *
   * @param podCount Number of pods to kill
   */
  public void setPodCount(int podCount) {
    this.podCount = podCount;
  }
  /**
   * The deployment script waitForDeployToFinish method polls until the new deployment is running
   * successfully. This means that it is returning success to status checks and that the deployment
   * has switched over to the new image/application properties/etc. (if applicable).
   */
  public void fail() throws Exception {
    throw new UnsupportedOperationException(
        "waitForDeployToFinish must be overridden by sub-classes");
  }
}

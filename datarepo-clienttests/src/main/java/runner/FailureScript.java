package runner;

import java.util.List;
import runner.config.ApplicationSpecification;
import runner.config.ServerSpecification;

public abstract class FailureScript {

    /** Public constructor so that this class can be instantiated via reflection. */
    public FailureScript() {}

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

package runner.config;

public class FailureSpecification implements SpecificationInterface {
    public int kubernetesKillPodCount = 0;
    // public int kubernetesWaitBeforeKillingPod = 30;
    // public String kubernetesWaitBeforeKillingPodUnit= "SECONDS";

    FailureSpecification() {}

    /** Validate the Failure specification read in from the JSON file. */
    public void validate() {
        if (kubernetesKillPodCount <= 0) {
            // todo
            // then we want to check that the other parameters are set
            //throw new IllegalArgumentException("Number of initial Kubernetes pods must be >= 0");
        }
    }
}

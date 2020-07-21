package runner.config;

public class KubernetesSpecification implements SpecificationInterface {
  public int numberOfInitialPods = 1;

  KubernetesSpecification() {}

  /** Validate the Kubernetes specification read in from the JSON file. */
  public void validate() {
    if (numberOfInitialPods <= 0) {
      throw new IllegalArgumentException("Number of initial Kubernetes pods must be >= 0");
    }
  }
}

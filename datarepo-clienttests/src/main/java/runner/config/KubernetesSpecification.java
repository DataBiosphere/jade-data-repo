package runner.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KubernetesSpecification implements SpecificationInterface {
  private static final Logger LOG = LoggerFactory.getLogger(KubernetesSpecification.class);

  public int numberOfInitialPods = 1;

  KubernetesSpecification() {}

  /** Validate the Kubernetes specification read in from the JSON file. */
  public void validate() {
    if (numberOfInitialPods <= 0) {
      throw new IllegalArgumentException("Number of initial Kubernetes pods must be >= 0");
    }
  }

  public void display() {
    LOG.info("Kubernetes: ");
    LOG.info("  numberOfInitialPods: {}", numberOfInitialPods);
  }
}

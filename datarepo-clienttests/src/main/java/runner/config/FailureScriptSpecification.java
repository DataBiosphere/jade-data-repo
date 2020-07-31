package runner.config;

import com.fasterxml.jackson.databind.*;
import java.io.*;
import utils.*;

public class FailureScriptSpecification implements SpecificationInterface {
  public int podCount = 0;
  // public int kubernetesWaitBeforeKillingPod = 30;
  // public String kubernetesWaitBeforeKillingPodUnit= "SECONDS";

  public static final String failurePackage = "failureconfigs";

  FailureScriptSpecification() {}

  // todo add method description
  public static FailureScriptSpecification fromJSONFile(String failureScriptName) throws Exception {
    // use Jackson to map the stream contents to a TestConfiguration object
    ObjectMapper objectMapper = new ObjectMapper();

    // read in the server file
    InputStream inputStream = FileUtils.getJSONFileHandle(failurePackage + "/" + failureScriptName);
    return objectMapper.readValue(inputStream, FailureScriptSpecification.class);
  }

  /** Validate the Failure specification read in from the JSON file. */
  public void validate() {
    if (podCount <= 0) {
      // todo
      // then we want to check that the other parameters are set
      // throw new IllegalArgumentException("Number of initial Kubernetes pods must be >= 0");
    }
  }
}

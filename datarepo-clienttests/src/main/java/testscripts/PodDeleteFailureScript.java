package testscripts;

import bio.terra.datarepo.client.ApiClient;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.TestScript;

// In my first implementation, I had this script in it's own "failurescripts" folder
// and I created a "FailureScript" class
// I think it was overkill b/c it's really the same as a testScript, but it is a bit less user
// friendly
//
// [Side effect: in the case of a separate "FailureScript" class, I also had to create
// a ScriptInterface for both FailureScript and TestScript to inherit from in order to share
// the user journey thread pool]

public class PodDeleteFailureScript extends TestScript {
  private static final Logger logger = LoggerFactory.getLogger(PodDeleteFailureScript.class);

  private int repeatFailureCount = 1;
  private int secondsWaitBeforeFailure = 30;

  public void setParameters(List<String> parameters) {

    if (parameters == null || parameters.size() != 2) {
      throw new IllegalArgumentException(
          "Must 3 parameters in the parameters list: repeatFailureCount, timeWaitBeforeFailure & unit");
    }
    repeatFailureCount = Integer.parseInt(parameters.get(0));
    secondsWaitBeforeFailure = Integer.parseInt(parameters.get(1));

    if (repeatFailureCount <= 0) {
      throw new IllegalArgumentException("Total failure counts must be >=0.");
    }

    if (secondsWaitBeforeFailure <= 0) {
      throw new IllegalArgumentException("Time to wait for each failure must be >=0.");
    }
  }

  @Override
  public void userJourney(ApiClient apiClient) throws Exception {
    for (int i = 0; i < repeatFailureCount; i++) {
      TimeUnit.SECONDS.sleep(secondsWaitBeforeFailure);
      // todo actually add the delete pod call!!
      logger.info("FAILING HERE!");
    }
  }
}

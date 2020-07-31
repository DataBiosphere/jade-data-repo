package testscripts;

import bio.terra.datarepo.client.ApiClient;
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

// todo: this is just a skeleton for now!
public class PodDeleteFailureScript extends TestScript {
  private static final Logger logger = LoggerFactory.getLogger(PodDeleteFailureScript.class);

  // TODO set parameters -> how many pods to kill? How long do we wait before killing them?

  @Override
  public void userJourney(ApiClient apiClient) throws Exception {
    TimeUnit.SECONDS.sleep(30);
    // todo actually add the delete pod call!!
    logger.info("FAILING HERE!");
  }
}

package common.commands;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.TestRunner;
import runner.config.ServerSpecification;
import runner.config.TestConfiguration;

public class LockAndRunTest {
  private static final Logger logger = LoggerFactory.getLogger(LockAndRunTest.class);

  public static void main(String[] args) throws Exception {
    if (args.length != 2) { // if no args specified or invalid number of args specified, print help
      PrintHelp.printHelp();
      return;
    }

    LockNamespace.lockNamespace();
    unlockShutdownHook();

    // execute a test configuration or suite
    boolean isFailure;
    isFailure = TestRunner.runTest(args[0], args[1]);
    if (isFailure) {
      System.exit(1);
    }
  }

  public static ServerSpecification getServer() throws Exception {
    // read in the server file
    String serverEnvVar = TestConfiguration.readServerEnvironmentVariable();
    if (serverEnvVar == null) {
      throw new Exception(
          TestConfiguration.serverFileEnvironmentVarName + " env variable must be defined");
    }
    return ServerSpecification.fromJSONFile(serverEnvVar);
  }

  public static void unlockShutdownHook() {
    Runtime.getRuntime()
        .addShutdownHook(
            new Thread(
                () -> {
                  try {
                    UnlockNamespace.unlockNamespace();
                  } catch (Exception e) {
                    logger.error("Unable to unlock.");
                  }
                }));
    logger.debug("Running unlock...");
  }
}

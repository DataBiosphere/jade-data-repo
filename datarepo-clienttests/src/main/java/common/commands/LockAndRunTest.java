package common.commands;

import runner.TestRunner;
import runner.config.ServerSpecification;
import runner.config.TestConfiguration;

public class LockAndRunTest {
  public static void main(String[] args) throws Exception {
    if (args.length != 2) { // if no args specified or invalid number of args specified, print help
      PrintHelp.printHelp();
      return;
    }
    // lock namespace
    LockNamespace.lockNamespace();
    // execute a test configuration or suite
    boolean isFailure;
    try {
      isFailure = TestRunner.runTest(args[0], args[1]);
    } catch (Exception ex) {
      // unlock
      UnlockNamespace.unlockNamespace();
      throw ex;
    }
    UnlockNamespace.unlockNamespace();
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
}

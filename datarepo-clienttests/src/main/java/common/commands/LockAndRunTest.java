package common.commands;

import runner.TestRunner;

public class LockAndRunTest {
  public static void main(String[] args) throws Exception {
    if (args.length != 2) { // if no args specified or invalid number of args specified, print help
      PrintHelp.printHelp();
      return;
    }
    // lock namespace
    LockNamespace.main(args);
    // execute a test configuration or suite
    boolean isFailure;
    try {
      isFailure = TestRunner.runTest(args[0], args[1]);
    } catch (Exception ex) {
      // unlock
      UnlockNamespace.main(args);
      throw ex;
    }
    // unlock
    UnlockNamespace.main(args);
    if (isFailure) {
      System.exit(1);
    }
  }
}

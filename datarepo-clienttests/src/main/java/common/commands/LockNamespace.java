package common.commands;

import common.utils.ProcessUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LockNamespace {
  private static final Logger logger = LoggerFactory.getLogger(LockNamespace.class);

  public static void main(String[] args) throws Exception {
    lockNamespace();
  }

  public static void lockNamespace() throws Exception {
    String namespace = LockAndRunTest.getServer().namespace;
    logger.info("Lock namespace by creating secret named " + namespace + "-inuse");
    List<String> scriptArgs = new ArrayList<>();
    scriptArgs.add("tools/namespaceLock.sh");
    scriptArgs.add(namespace);
    Process fetchCredentialsProc = ProcessUtils.executeCommand("sh", scriptArgs);
    List<String> cmdOutputLines = ProcessUtils.waitForTerminateAndReadStdout(fetchCredentialsProc);
    fetchCredentialsProc.waitFor(5, TimeUnit.SECONDS);
    if (fetchCredentialsProc.exitValue() > 0) {
      throw new Exception(
          "FAILURE: Failed to acquire lock for namespace "
              + namespace
              + "\n"
              + "REASON: Namespace already in use.\n"
              + "OVERRIDE: Run './gradlew unlockNamespace' to clear lock\n"
              + "(Warning! Check w/ team & github actions to assert no one else using namespace)");
    }
    for (String cmdOutputLine : cmdOutputLines) {
      logger.info(cmdOutputLine);
    }
  }
}

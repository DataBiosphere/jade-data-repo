package common.commands;

import common.utils.ProcessUtils;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UnlockNamespace {
  private static final Logger logger = LoggerFactory.getLogger(UnlockNamespace.class);

  public static void main(String[] args) throws Exception {
    unlockNamespace();
  }

  public static void unlockNamespace() throws Exception {
    String namespace = LockAndRunTest.getServer().namespace;
    logger.info("Unlock namespace by deleting secret named " + namespace + "-inuse");
    List<String> scriptArgs = new ArrayList<>();
    scriptArgs.add("tools/deleteNamespaceLock.sh");
    scriptArgs.add(namespace);
    Process fetchCredentialsProc = ProcessUtils.executeCommand("sh", scriptArgs);
    List<String> cmdOutputLines = ProcessUtils.waitForTerminateAndReadStdout(fetchCredentialsProc);
    for (String cmdOutputLine : cmdOutputLines) {
      logger.info(cmdOutputLine);
    }
  }
}

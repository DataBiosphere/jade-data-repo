package common.commands;

import common.utils.ProcessUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class LockNamespace {
  public static void main(String[] args) throws Exception {
    lockNamespace();
  }

  public static void lockNamespace() throws Exception {
    String namespace = LockAndRunTest.getServer().namespace;
    System.out.println("Lock namespace by creating secret named " + namespace + "-inuse");
    List<String> scriptArgs = new ArrayList<>();
    scriptArgs.add("tools/namespaceLock.sh");
    scriptArgs.add(namespace);
    Process fetchCredentialsProc = ProcessUtils.executeCommand("sh", scriptArgs);
    System.out.println(
        "If you're stuck here... Make sure you're on the VPN and have run ./render-configs from jade-data-repo directory");
    List<String> cmdOutputLines = ProcessUtils.waitForTerminateAndReadStdout(fetchCredentialsProc);
    fetchCredentialsProc.waitFor(5, TimeUnit.SECONDS);
    if (fetchCredentialsProc.exitValue() > 0) {
      throw new Exception("FAILURE: Failed to acquire lock for namespace " + namespace);
    }
    for (String cmdOutputLine : cmdOutputLines) {
      System.out.println(cmdOutputLine);
    }
  }
}

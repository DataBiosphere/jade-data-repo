package common.commands;

import common.utils.ProcessUtils;
import java.util.ArrayList;
import java.util.List;

public class UnlockNamespace {
  public static void main(String[] args) throws Exception {
    unlockNamespace();
  }

  public static void unlockNamespace() throws Exception {
    String namespace = LockAndRunTest.getServer().namespace;
    System.out.println("unlock namespace by deleting secret named " + namespace + "-inuse");
    List<String> scriptArgs = new ArrayList<>();
    scriptArgs.add("tools/deleteNamespaceLock.sh");
    scriptArgs.add(namespace);
    Process fetchCredentialsProc = ProcessUtils.executeCommand("sh", scriptArgs);
    List<String> cmdOutputLines = ProcessUtils.waitForTerminateAndReadStdout(fetchCredentialsProc);
    for (String cmdOutputLine : cmdOutputLines) {
      System.out.println(cmdOutputLine);
    }
  }
}

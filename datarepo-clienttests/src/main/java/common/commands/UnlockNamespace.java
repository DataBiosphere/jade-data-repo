package common.commands;

import common.utils.ProcessUtils;
import common.utils.TestConfigurationUtils;
import java.util.ArrayList;
import java.util.List;

public class UnlockNamespace {
  public static void main(String[] args) throws Exception {
    String namespace = TestConfigurationUtils.getNamespace();
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

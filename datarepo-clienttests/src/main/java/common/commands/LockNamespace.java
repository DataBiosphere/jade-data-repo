package common.commands;

import common.utils.TestConfigurationUtils;
import common.utils.ProcessUtils;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;

public class LockNamespace {
    public static void main(String[] args) throws Exception {
        String namespace = TestConfigurationUtils.getNamespace();
        System.out.println("Lock namespace by creating secret named " + namespace + "-inuse");
        List<String> scriptArgs = new ArrayList<>();
        scriptArgs.add("tools/namespaceLock.sh");
        scriptArgs.add(namespace);
        Process fetchCredentialsProc = ProcessUtils.executeCommand("sh", scriptArgs);
        List<String> cmdOutputLines = ProcessUtils.waitForTerminateAndReadStdout(fetchCredentialsProc);
        fetchCredentialsProc.waitFor(30, TimeUnit.SECONDS);
        if (fetchCredentialsProc.exitValue() > 0) {
            throw new Exception("FAILURE: Failed to acquire lock for namespace " + namespace);
        }
        for (String cmdOutputLine : cmdOutputLines) {
            System.out.println(cmdOutputLine);
        }
    }
}

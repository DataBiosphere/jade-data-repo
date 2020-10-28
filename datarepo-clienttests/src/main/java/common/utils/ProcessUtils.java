package common.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ProcessUtils {
  private static final Logger logger = LoggerFactory.getLogger(ProcessUtils.class);

  private ProcessUtils() {}

  /**
   * Executes a command in a separate process from the current working directory (i.e. the same
   * place as this Java process is running).
   *
   * @param cmd the main command (e.g. "ls")
   * @param cmdArgs a list of the command line arguments (e.g. ["-l","."])
   * @return the process handle
   */
  public static Process executeCommand(String cmd, List<String> cmdArgs) throws IOException {
    return executeCommand(cmd, cmdArgs, null, null);
  }

  /**
   * Executes a command in a separate process from the given working directory, with the given
   * environment variables set beforehand.
   *
   * @param cmd the main command (e.g. "ls")
   * @param cmdArgs a list of the command line arguments (e.g. ["-l","-a","."])
   * @param workingDirectory the working directory to launch the process from
   * @param envVars the environment variables to set or overwrite if already defined
   * @return the process handle
   */
  public static Process executeCommand(
      String cmd, List<String> cmdArgs, File workingDirectory, Map<String, String> envVars)
      throws IOException {
    // build the full command string
    cmdArgs.add(0, cmd);
    logger.debug("Executing command: {}", String.join(" ", cmdArgs));

    // build and run process from the specified working directory
    ProcessBuilder procBuilder = new ProcessBuilder(cmdArgs);
    if (workingDirectory != null) {
      procBuilder.directory(workingDirectory);
    }
    if (envVars != null) {
      Map<String, String> procEnvVars = procBuilder.environment();
      for (Map.Entry<String, String> envVar : envVars.entrySet()) {
        procEnvVars.put(envVar.getKey(), envVar.getValue());
      }
    }
    return procBuilder.start();
  }

  /**
   * Reads in all lines that the given process writes to stdout. Blocks until the process
   * terminates.
   *
   * @param proc the process handle
   * @return the list of lines written to stdout
   */
  public static List<String> waitForTerminateAndReadStdout(Process proc) throws IOException {
    return readStdout(proc, -1);
  }

  /**
   * Reads in all lines that the given process writes to stdout, up to the given maximum number of
   * lines. Blocks until the given number of lines are written to stdout.
   *
   * @param proc the process handle
   * @param numLines the maximum number of lines to read. any negative number means to read until
   *     the process terminates
   * @return the list of lines written to stdout
   */
  public static List<String> readStdout(Process proc, long numLines) throws IOException {
    // read in all lines written to stdout
    BufferedReader bufferedReader =
        new BufferedReader(new InputStreamReader(proc.getInputStream(), Charset.defaultCharset()));
    String outputLine;
    List<String> outputLines = new ArrayList<>();

    while ((outputLine = bufferedReader.readLine()) != null && numLines != 0) {
      outputLines.add(outputLine);
      numLines--;
    }
    bufferedReader.close();

    return outputLines;
  }

  /**
   * Kill the process and wait for it to terminate.
   *
   * @param proc the process handle
   * @param timeoutSeconds the maximum amount of time, in seconds, to wait for the process to
   *     terminate
   * @return true if the process terminated successfully, false otherwise.
   */
  public static boolean killProcessAndWaitForTermination(Process proc, long timeoutSeconds)
      throws InterruptedException {
    if (timeoutSeconds < 1) {
      throw new IllegalArgumentException(
          "Timeout (seconds) for process termination must be at least 1");
    }

    // first try to destroy the process nicely
    proc.destroy();

    // check to see if it terminated.
    if (!proc.isAlive()) {
      return true;
    }
    // if not, sleep one second
    TimeUnit.SECONDS.sleep(1);

    // check again to see if it's terminated.
    if (!proc.isAlive()) {
      return true;
    }
    // if not, try to destroy the process forcibly and wait for the remainder of the given timeout
    proc.destroyForcibly();
    return proc.waitFor(timeoutSeconds - 1, TimeUnit.SECONDS);
  }
}

package scripts.deploymentscripts;

import bio.terra.datarepo.api.UnauthenticatedApi;
import bio.terra.datarepo.client.ApiClient;
import com.google.api.client.http.HttpStatusCodes;
import common.utils.ProcessUtils;
import java.io.File;
import java.net.URL;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.DeploymentScript;
import runner.config.ApplicationSpecification;
import runner.config.ServerSpecification;

public class LaunchLocalProcess extends DeploymentScript {
  private static final Logger logger = LoggerFactory.getLogger(LaunchLocalProcess.class);

  private String jadedatarepoFilePath;

  private ServerSpecification serverSpecification;
  private ApplicationSpecification applicationSpecification;

  private Process serverProcess;

  // these timeout values are used for both process launch and terminate
  private static int maximumSecondsToWaitForProcess = 100;
  private static int secondsIntervalToPollForProcess = 5;

  /** Public constructor so that this class can be instantiated via reflection. */
  public LaunchLocalProcess() {
    super();
  }

  /**
   * Expects a single parameter: a local URL (i.e. file://) that points to the top-level directory
   * where jade-data-repo code is checked out.
   *
   * @param parameters list of string parameters supplied by the test configuration
   */
  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() < 1) {
      throw new IllegalArgumentException(
          "Must provide a file path for the jade-data-repo code directory in the parameters list");
    } else {
      jadedatarepoFilePath = parameters.get(0);
      logger.debug("jade-data-repo code directory: {}", jadedatarepoFilePath);
    }
  }

  /**
   * 1. Check that there is no server already running locally. 2. Check that the pointer to the
   * local codebase is valid. 3. Launch the server from the codebase directory with the gradle
   * bootRun task.
   *
   * @param server the server configuration supplied by the test configuration
   * @param app the application configuration supplied by the test configuration
   */
  public void deploy(ServerSpecification server, ApplicationSpecification app) throws Exception {
    // store these on the instance to avoid passing them around to all the helper methods
    serverSpecification = server;
    applicationSpecification = app;

    // confirm that the local server process does NOT respond successfully to a status request
    // if it does, that means there's already a local server running that we can't control
    // so error out here
    logger.debug(
        "Checking service status endpoint to confirm that there is no local server already running");
    boolean statusRequestOK;
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(serverSpecification.datarepoUri);
    UnauthenticatedApi unauthenticatedApi = new UnauthenticatedApi(apiClient);
    // call the unauthenticated status endpoint
    try {
      unauthenticatedApi.serviceStatus();
      int httpStatus = unauthenticatedApi.getApiClient().getStatusCode();
      statusRequestOK = HttpStatusCodes.isSuccess(httpStatus);
    } catch (Exception ex) {
      statusRequestOK = false;
    }
    if (statusRequestOK) {
      throw new RuntimeException(
          "There is already a local server running. It needs to be shutdown before the TestRunner can deploy locally.");
    }

    // make sure the directory exists
    File jadedatarepoDirectory = Paths.get(new URL(jadedatarepoFilePath).toURI()).toFile();
    if (!jadedatarepoDirectory.exists() || !jadedatarepoDirectory.isDirectory()) {
      throw new IllegalArgumentException(
          "The file path for the jade-data-repo code directory is invalid: "
              + jadedatarepoDirectory.getAbsolutePath());
    }

    // launch the server locally with the gradle bootRun task
    logger.debug("Launching the server locally with the gradle bootRun task");
    ArrayList<String> gradleCmdArgs = new ArrayList<>();
    gradleCmdArgs.add(":bootRun");
    Map<String, String> envVars = buildEnvVarsMap();
    serverProcess =
        ProcessUtils.executeCommand("./gradlew", gradleCmdArgs, jadedatarepoDirectory, envVars);
  }

  /** 1. Poll the unauthenticated status endpoint until it returns success. */
  public void waitForDeployToFinish() throws Exception {
    int pollCtr = Math.floorDiv(maximumSecondsToWaitForProcess, secondsIntervalToPollForProcess);

    // wait for the local server process to respond successfully to a status request
    logger.debug(
        "Waiting for the local server process to respond successfully to a status request");
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(serverSpecification.datarepoUri);
    UnauthenticatedApi unauthenticatedApi = new UnauthenticatedApi(apiClient);
    while (pollCtr >= 0) {
      // call the unauthenticated status endpoint
      try {
        unauthenticatedApi.serviceStatus();
        int httpStatus = unauthenticatedApi.getApiClient().getStatusCode();
        logger.debug("Service status: {}", httpStatus);
        if (HttpStatusCodes.isSuccess(httpStatus)) {
          break;
        }
      } catch (Exception ex) {
        logger.debug("Exception caught while checking service status", ex);
      }

      TimeUnit.SECONDS.sleep(secondsIntervalToPollForProcess);
      pollCtr--;
    }

    // print out the active profiles (for debugging)
    List<String> cmdOutputLines = ProcessUtils.readStdout(serverProcess, 25);
    for (String cmdOutputLine : cmdOutputLines) {
      if (cmdOutputLine.contains("bio.terra.Main: The following profiles are active")) {
        logger.debug(cmdOutputLine);
      }
    }
  }

  /**
   * 1. Kill the process and wait for it to terminate, up to a timeout. 2. Check that the
   * unauthenticated status endpoint does NOT respond successfully.
   */
  public void teardown() throws Exception {
    logger.debug("Killing the local process");
    boolean processKilled =
        ProcessUtils.killProcessAndWaitForTermination(
            serverProcess, maximumSecondsToWaitForProcess);
    logger.debug("Server process killed: {}", processKilled);

    // confirm that the local server process does NOT respond successfully to a status request
    logger.debug("Checking service status endpoint to confirm the local server is shut down");
    boolean statusRequestOK;
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(serverSpecification.datarepoUri);
    UnauthenticatedApi unauthenticatedApi = new UnauthenticatedApi(apiClient);
    // call the unauthenticated status endpoint
    try {
      unauthenticatedApi.serviceStatus();
      int httpStatus = unauthenticatedApi.getApiClient().getStatusCode();
      statusRequestOK = HttpStatusCodes.isSuccess(httpStatus);
    } catch (Exception ex) {
      statusRequestOK = false;
    }

    if (statusRequestOK) {
      logger.error(
          "Local server shutdown failed; it is still responding successfully to status requests");
    } else {
      logger.debug("Status request failed, as expected");
    }
  }

  /**
   * Build a map of environment variables specified by the application configuration or required by
   * the test runner.
   *
   * @return the map
   */
  private Map<String, String> buildEnvVarsMap() {
    Map<String, String> envVars = new HashMap<>();

    // TODO: instead of forcing this, consider read the value from application.properties file and
    // append perftest?
    // force the active Spring profiles to google and perftest
    envVars.put("SPRING_PROFILES_ACTIVE", "google,perftest");

    // always set the following testing-related environment variables
    envVars.put("DB_STAIRWAY_FORCECLEAN", "true");
    envVars.put("GOOGLE_ALLOWREUSEEXISTINGBUCKETS", "true");

    // set the following environment variables from the application specification object
    envVars.put("DB_MIGRATE_DROPALLONSTART", String.valueOf(serverSpecification.dbDropAllOnStart));
    envVars.put(
        "DATAREPO_MAXSTAIRWAYTHREADS", String.valueOf(applicationSpecification.maxStairwayThreads));
    envVars.put(
        "DATAREPO_MAXBULKFILELOAD", String.valueOf(applicationSpecification.maxBulkFileLoad));
    envVars.put(
        "DATAREPO_MAXBULKFILELOADARRAY",
        String.valueOf(applicationSpecification.maxBulkFileLoadArray));
    envVars.put(
        "DATAREPO_MAXDATASETINGEST", String.valueOf(applicationSpecification.maxDatasetIngest));
    envVars.put(
        "DATAREPO_LOADCONCURRENTFILES",
        String.valueOf(applicationSpecification.loadConcurrentFiles));
    envVars.put(
        "DATAREPO_LOADDRIVERWAITSECONDS",
        String.valueOf(applicationSpecification.loadDriverWaitSeconds));
    envVars.put(
        "DATAREPO_LOADCONCURRENTINGESTS",
        String.valueOf(applicationSpecification.loadConcurrentIngests));
    envVars.put(
        "DATAREPO_LOADHISTORYCOPYCHUNKSIZE",
        String.valueOf(applicationSpecification.loadHistoryCopyChunkSize));
    envVars.put(
        "DATAREPO_LOADHISTORYWAITSECONDS",
        String.valueOf(applicationSpecification.loadHistoryWaitSeconds));

    return envVars;
  }
}

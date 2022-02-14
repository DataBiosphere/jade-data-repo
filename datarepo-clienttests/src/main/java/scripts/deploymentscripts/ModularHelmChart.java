package scripts.deploymentscripts;

import bio.terra.datarepo.api.UnauthenticatedApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.ApiException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import com.google.api.client.http.HttpStatusCodes;
import common.utils.FileUtils;
import common.utils.ProcessUtils;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.DeploymentScript;
import runner.config.ApplicationSpecification;
import runner.config.ServerSpecification;

public class ModularHelmChart extends DeploymentScript {
  private static final Logger logger = LoggerFactory.getLogger(ModularHelmChart.class);

  private String helmApiFilePath;

  private ServerSpecification serverSpecification;
  private ApplicationSpecification applicationSpecification;

  private static int maximumSecondsToWaitForDeploy = 1000;
  private static int secondsIntervalToPollForDeploy = 15;

  /** Public constructor so that this class can be instantiated via reflection. */
  public ModularHelmChart() {
    super();
  }

  /**
   * Expects a single parameter: a URL to the Helm datarepo-api definition YAML. The URL may point
   * to a local (i.e. file://) or remote (e.g. https://) file.
   *
   * @param parameters list of string parameters supplied by the test configuration
   */
  public void setParameters(List<String> parameters) throws Exception {
    if (parameters == null || parameters.size() < 1) {
      throw new IllegalArgumentException(
          "Must provide a file path for the Helm API definition YAML in the parameters list");
    } else {
      helmApiFilePath = parameters.get(0);
      logger.debug("Helm API definition YAML: {}", helmApiFilePath);
    }
  }

  /**
   * 1. Copy the specified Helm datarepo-api definition YAML file to the current working directory.
   * 2. Modify the copied original YAML file to include the environment variables specified in the
   * application configuration. 3. Delete the existing API deployment using Helm. 4. Re-install the
   * API deployment using the modified datarepo-api definition YAML file. 5. Delete the two
   * temporary files created in the current working directory.
   *
   * @param server the server configuration supplied by the test configuration
   * @param app the application configuration supplied by the test configuration
   */
  public void deploy(ServerSpecification server, ApplicationSpecification app) throws Exception {
    // store these on the instance to avoid passing them around to all the helper methods
    serverSpecification = server;
    applicationSpecification = app;

    // get file handle to original/template API deployment Helm YAML file
    File originalApiYamlFile =
        FileUtils.createCopyOfFileFromURL(new URL(helmApiFilePath), "datarepo-api_ORIGINAL.yaml");

    // modify the original/template YAML file and write the output to a new file
    File modifiedApiYamlFile = FileUtils.createNewFile(new File("datarepo-api_MODIFIED.yaml"));
    parseAndModifyApiYamlFile(originalApiYamlFile, modifiedApiYamlFile);

    // delete the existing API deployment
    // e.g. helm namespace delete mm-jade-datarepo-api --namespace mm
    ArrayList<String> deleteCmdArgs = new ArrayList<>();
    deleteCmdArgs.add("namespace");
    deleteCmdArgs.add("delete");
    deleteCmdArgs.add(serverSpecification.namespace + "-jade-datarepo-api");
    deleteCmdArgs.add("--namespace");
    deleteCmdArgs.add(serverSpecification.namespace);
    Process helmDeleteProc = ProcessUtils.executeCommand("helm", deleteCmdArgs);
    List<String> cmdOutputLines = ProcessUtils.waitForTerminateAndReadStdout(helmDeleteProc);
    for (String cmdOutputLine : cmdOutputLines) {
      logger.debug(cmdOutputLine);
    }

    // list the available deployments (for debugging)
    // e.g. helm ls --namespace mm
    ArrayList<String> listCmdArgs = new ArrayList<>();
    listCmdArgs.add("ls");
    listCmdArgs.add("--namespace");
    listCmdArgs.add(serverSpecification.namespace);
    Process helmListProc = ProcessUtils.executeCommand("helm", listCmdArgs);
    cmdOutputLines = ProcessUtils.waitForTerminateAndReadStdout(helmListProc);
    for (String cmdOutputLine : cmdOutputLines) {
      logger.debug(cmdOutputLine);
    }

    // install/upgrade the API deployment using the modified YAML file we just generated
    // e.g. helm namespace upgrade mm-jade-datarepo-api datarepo-helm/datarepo-api --install
    // --namespace mm -f datarepo-api_MODIFIED.yaml
    ArrayList<String> installCmdArgs = new ArrayList<>();
    installCmdArgs.add("namespace");
    installCmdArgs.add("upgrade");
    installCmdArgs.add(serverSpecification.namespace + "-jade-datarepo-api");
    installCmdArgs.add("datarepo-helm/datarepo-api");
    installCmdArgs.add("--install");
    installCmdArgs.add("--namespace");
    installCmdArgs.add(serverSpecification.namespace);
    installCmdArgs.add("-f");
    installCmdArgs.add(modifiedApiYamlFile.getAbsolutePath());
    Process helmUpgradeProc = ProcessUtils.executeCommand("helm", installCmdArgs);
    cmdOutputLines = ProcessUtils.waitForTerminateAndReadStdout(helmUpgradeProc);
    for (String cmdOutputLine : cmdOutputLines) {
      logger.debug(cmdOutputLine);
    }

    // delete the two temp YAML files created above
    boolean originalYamlFileDeleted = originalApiYamlFile.delete();
    if (!originalYamlFileDeleted) {
      throw new RuntimeException(
          "Error deleting the _ORIGINAL YAML file: " + originalApiYamlFile.getAbsolutePath());
    }
    boolean modifiedYamlFileDeleted = modifiedApiYamlFile.delete();
    if (!modifiedYamlFileDeleted) {
      throw new RuntimeException(
          "Error deleting the _MODIFIED YAML file: " + modifiedApiYamlFile.getAbsolutePath());
    }
  }

  /**
   * 1. Poll the deployment status with Helm until it reports that the datarepo-api is "deployed".
   * 2. Poll the unauthenticated status endpoint until it returns success.
   *
   * <p>Waiting for the deployment to be ready to respond to API requests, often takes a long time
   * (~5-15 minutes) because we deleted the deployment before re-installing it. This restarts the
   * oidc-proxy which is what's holding things up. If we skip the delete deployment and just
   * re-install, then this method usually returns much quicker (~1-5 minutes). We need to do the
   * delete in order for the databases (Stairway and Data Repo) to be wiped because of how they
   * decide which pod will do the database migration.
   */
  public void waitForDeployToFinish() throws Exception {
    int pollCtr = Math.floorDiv(maximumSecondsToWaitForDeploy, secondsIntervalToPollForDeploy);

    // first wait for the datarepo-api deployment to report "deployed" by helm ls
    logger.debug("Waiting for Helm to report datarepo-api as deployed");
    boolean foundHelmStatusDeployed = false;
    while (pollCtr >= 0) {
      // list the available deployments
      // e.g. helm ls --namespace mm
      ArrayList<String> listCmdArgs = new ArrayList<>();
      listCmdArgs.add("ls");
      listCmdArgs.add("--namespace");
      listCmdArgs.add(serverSpecification.namespace);
      Process helmListProc = ProcessUtils.executeCommand("helm", listCmdArgs);
      List<String> cmdOutputLines = ProcessUtils.waitForTerminateAndReadStdout(helmListProc);
      for (String cmdOutputLine : cmdOutputLines) {
        logger.debug(cmdOutputLine);
      }

      for (String cmdOutputLine : cmdOutputLines) {
        if (cmdOutputLine.startsWith(serverSpecification.namespace + "-jade-datarepo-api")) {
          if (cmdOutputLine.contains("deployed")) {
            foundHelmStatusDeployed = true;
            break;
          }
        }
      }

      if (foundHelmStatusDeployed) {
        break;
      }

      TimeUnit.SECONDS.sleep(secondsIntervalToPollForDeploy);
      pollCtr--;
    }

    // then wait for the datarepo-api deployment to respond successfully to a status request
    logger.debug("Waiting for the datarepo-api to respond successfully to a status request");
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
      } catch (ApiException apiEx) {
        logger.debug("Exception caught while checking service status", apiEx);
      }

      TimeUnit.SECONDS.sleep(secondsIntervalToPollForDeploy);
      pollCtr--;
    }
  }

  /**
   * Parse the original datarepo-api YAML file and modify or add environment variables. Write the
   * result to the specified output file.
   *
   * @param inputFile the original datarepo-api YAML file
   * @param outputFile the modified datarepo-api YAML file
   */
  private void parseAndModifyApiYamlFile(File inputFile, File outputFile) throws IOException {
    ObjectMapper objectMapper = new YAMLMapper();
    JsonNode inputTree = objectMapper.readTree(inputFile);
    ObjectNode envSubTree = (ObjectNode) inputTree.get("env");
    if (envSubTree == null) {
      throw new IllegalArgumentException("Error parsing datarepo-api YAML file");
    }

    // confirm that the expected environment variables/application properties are set
    final List<String> environmentSpecificVariables =
        Arrays.asList(
            "DB_DATAREPO_USERNAME",
            "DB_STAIRWAY_USERNAME",
            "DB_DATAREPO_URI",
            "DB_STAIRWAY_URI",
            "SPRING_PROFILES_ACTIVE");
    for (String var : environmentSpecificVariables) {
      JsonNode varValue = envSubTree.get(var);
      if (varValue == null) {
        throw new IllegalArgumentException(
            "Expected environment variable/application property not found in datarepo-api YAML file: "
                + var);
      }
    }

    // add the perftest profile to the SPRING_PROFILES_ACTIVE if it isn't already included
    String activeSpringProfiles = envSubTree.get("SPRING_PROFILES_ACTIVE").asText();
    if (!activeSpringProfiles.contains("perftest")) {
      envSubTree.put("SPRING_PROFILES_ACTIVE", activeSpringProfiles + ",perftest");
    }

    // always set the following testing-related environment variables
    envSubTree.put("DB_STAIRWAY_FORCECLEAN", "true");
    envSubTree.put("GOOGLE_ALLOWREUSEEXISTINGBUCKETS", "true");

    // set the following environment variables from the application specification object
    // make sure the values are Strings so that they will be quoted in the Helm chart
    // otherwise, Helm may convert numbers to scientific notation, which breaks Spring's ability to
    // read them in as application properties

    envSubTree.put(
        "DB_MIGRATE_DROPALLONSTART", String.valueOf(serverSpecification.dbDropAllOnStart));
    envSubTree.put(
        "DATAREPO_MAXSTAIRWAYTHREADS", String.valueOf(applicationSpecification.maxStairwayThreads));
    envSubTree.put(
        "DATAREPO_MAXBULKFILELOAD", String.valueOf(applicationSpecification.maxBulkFileLoad));
    envSubTree.put(
        "DATAREPO_MAXBULKFILELOADARRAY",
        String.valueOf(applicationSpecification.maxBulkFileLoadArray));
    envSubTree.put(
        "DATAREPO_MAXDATASETINGEST", String.valueOf(applicationSpecification.maxDatasetIngest));
    envSubTree.put(
        "DATAREPO_LOADCONCURRENTFILES",
        String.valueOf(applicationSpecification.loadConcurrentFiles));
    envSubTree.put(
        "DATAREPO_LOADCONCURRENTINGESTS",
        String.valueOf(applicationSpecification.loadConcurrentIngests));
    envSubTree.put(
        "DATAREPO_LOADDRIVERWAITSECONDS",
        String.valueOf(applicationSpecification.loadDriverWaitSeconds));
    envSubTree.put(
        "DATAREPO_LOADHISTORYCOPYCHUNKSIZE",
        String.valueOf(applicationSpecification.loadHistoryCopyChunkSize));
    envSubTree.put(
        "DATAREPO_LOADHISTORYWAITSECONDS",
        String.valueOf(applicationSpecification.loadHistoryWaitSeconds));

    // write the modified tree out to disk
    objectMapper.writeValue(outputFile, inputTree);
  }
}

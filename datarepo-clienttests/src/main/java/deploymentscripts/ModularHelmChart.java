package deploymentscripts;

import bio.terra.datarepo.api.UnauthenticatedApi;
import bio.terra.datarepo.client.ApiClient;
import bio.terra.datarepo.client.ApiException;
import com.google.api.client.http.HttpStatusCodes;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URL;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import runner.DeploymentScript;
import runner.config.ApplicationSpecification;
import runner.config.ServerSpecification;
import utils.FileUtils;
import utils.ProcessUtils;

public class ModularHelmChart extends DeploymentScript {
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
      System.out.println("Helm API definition YAML: " + helmApiFilePath);
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
    File modifiedApiYamlFile = FileUtils.createNewFile("datarepo-api_MODIFIED.yaml");
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
      System.out.println(cmdOutputLine);
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
      System.out.println(cmdOutputLine);
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
      System.out.println(cmdOutputLine);
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
    System.out.println("Checking Helm status of datarepo-api deployment");
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
        System.out.println(cmdOutputLine);
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
    System.out.println("Checking service status endpoint");
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(serverSpecification.uri);
    UnauthenticatedApi unauthenticatedApi = new UnauthenticatedApi(apiClient);
    while (pollCtr >= 0) {
      // call the unauthenticated status endpoint
      try {
        unauthenticatedApi.serviceStatus();
        int httpStatus = unauthenticatedApi.getApiClient().getStatusCode();
        System.out.println("Service status: " + httpStatus);
        if (HttpStatusCodes.isSuccess(httpStatus)) {
          break;
        }
      } catch (ApiException apiEx) {
        System.out.println("Exception caught while checking service status: " + apiEx.getMessage());
      }

      TimeUnit.SECONDS.sleep(secondsIntervalToPollForDeploy);
      pollCtr--;
    }
  }

  /**
   * Loop through the original datarepo-api YAML file and modify or add environment variables. Write
   * the result to the specified output file.
   *
   * @param inputFile the original datarepo-api YAML file
   * @param outputFile the modified datarepo-api YAML file
   */
  private void parseAndModifyApiYamlFile(File inputFile, File outputFile) throws IOException {
    // the file line-by-line parsing below can be brittle because the YAML format is strictly
    // enforced
    BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(new FileInputStream(inputFile), Charset.defaultCharset()));
    BufferedWriter writer =
        new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(outputFile), Charset.defaultCharset()));

    // loop through the file looking for the "env" label
    // echo each line out verbatim to the output file
    String line;
    while ((line = reader.readLine()) != null) {
      // echo the line to the output file
      writer.write(line);
      writer.newLine();

      if (line.startsWith("env:")) {
        break;
      }
    }

    // at this point, we're in the env section
    // read in all values under this section, without echoing them back out immediately
    // build a map and pass it to a helper method to process and fill in
    Map<String, String> envVars = new HashMap<>();
    Pattern envVarPattern = Pattern.compile("^\\s+(.*?):\\s(.*)");
    while ((line = reader.readLine()) != null) {
      // check if we've reached the end of the env section
      if (!line.startsWith("  ")) {
        // update the environment variable map
        envVars = processEnvVars(envVars);

        // write the updated map to the output file
        for (Map.Entry<String, String> envVarEntry : envVars.entrySet()) {
          writer.write("  " + envVarEntry.getKey() + ": " + envVarEntry.getValue());
          writer.newLine();
        }

        writer.write(line);
        writer.newLine();
        break;
      }

      // parse this environment variable definition line into the map
      Matcher lineMatcher = envVarPattern.matcher(line);
      if (!lineMatcher.find()) {
        reader.close();
        writer.close();
        throw new RuntimeException(
            "Error parsing the datarepo-api/env section of the Helm YAML file: " + line);
      }

      // store the env var name -> value in the map
      envVars.put(lineMatcher.group(1), lineMatcher.group(2));
    }

    // at this point we're out of the env section
    // loop through the remaining lines, echoing each one out verbatim to the output file
    while ((line = reader.readLine()) != null) {
      writer.write(line);
      writer.newLine();
    }

    // flush and close the streams
    reader.close();
    writer.flush();
    writer.close();
  }

  // these environment variables are environment-specific and must be defined in the YAML already
  private static final List<String> environmentSpecificVariables =
      Arrays.asList(
          "GOOGLE_PROJECTID",
          "GOOGLE_SINGLEDATAPROJECTID",
          "DB_DATAREPO_USERNAME",
          "DB_STAIRWAY_USERNAME",
          "DB_DATAREPO_URI",
          "DB_STAIRWAY_URI",
          "SPRING_PROFILES_ACTIVE");

  /**
   * Modify the map of environment variables read from the original datarepo-api YAML file to
   * include the values specified by the application configuration or required by the test runner.
   *
   * @param envVars the original map
   * @return the modified map
   */
  private Map<String, String> processEnvVars(Map<String, String> envVars) {
    Map<String, String> modifiedEnvVars = new HashMap<>();
    for (String var : environmentSpecificVariables) {
      String varValue = envVars.get(var);
      if (varValue == null) {
        throw new IllegalArgumentException("Expected environment variable not found: " + var);
      }
      modifiedEnvVars.put(var, varValue);
    }

    // add the perftest profile to the SPRING_PROFILES_ACTIVE if it isn't already included
    String activeSpringProfiles = modifiedEnvVars.get("SPRING_PROFILES_ACTIVE");
    if (!activeSpringProfiles.contains("perftest")) {
      modifiedEnvVars.put("SPRING_PROFILES_ACTIVE", activeSpringProfiles + ",perftest");
    }

    // always set the following testing-related environment variables
    modifiedEnvVars.put("DB_STAIRWAY_FORCECLEAN", "true");
    modifiedEnvVars.put("DB_MIGRATE_DROPALLONSTART", "true");
    modifiedEnvVars.put("DATAREPO_GCS_ALLOWREUSEEXISTINGBUCKETS", "true");

    // set the following environment variables from the application specification object
    modifiedEnvVars.put(
        "DATAREPO_MAXSTAIRWAYTHREADS", "\"" + applicationSpecification.maxStairwayThreads + "\"");
    modifiedEnvVars.put(
        "DATAREPO_MAXBULKFILELOAD", "\"" + applicationSpecification.maxBulkFileLoad + "\"");
    modifiedEnvVars.put(
        "DATAREPO_LOADCONCURRENTFILES", "\"" + applicationSpecification.loadConcurrentFiles + "\"");
    modifiedEnvVars.put(
        "DATAREPO_LOADCONCURRENTINGESTS",
        "\"" + applicationSpecification.loadConcurrentIngests + "\"");
    modifiedEnvVars.put(
        "DATAREPO_LOADHISTORYCOPYCHUNKSIZE",
        "\"" + applicationSpecification.loadHistoryCopyChunkSize + "\"");
    modifiedEnvVars.put(
        "DATAREPO_LOADHISTORYWAITSECONDS",
        "\"" + applicationSpecification.loadHistoryWaitSeconds + "\"");

    return modifiedEnvVars;
  }
}

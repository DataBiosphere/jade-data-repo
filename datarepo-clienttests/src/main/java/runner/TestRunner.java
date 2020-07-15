package runner;

import bio.terra.datarepo.client.ApiClient;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import io.kubernetes.client.openapi.models.V1Deployment;
import io.kubernetes.client.openapi.models.V1Pod;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import runner.config.TestConfiguration;
import runner.config.TestScriptSpecification;
import runner.config.TestUserSpecification;
import utils.AuthenticationUtils;
import utils.FileUtils;
import utils.KubernetesClientUtils;

class TestRunner {

  TestConfiguration config;
  List<TestScript> scripts;
  List<ThreadPoolExecutor> threadPools;
  List<List<Future<UserJourneyResult>>> userJourneyFutureLists;
  List<UserJourneyResult> userJourneyResults;
  Map<String, ApiClient> apiClientsForUsers; // testUser -> apiClient
  // TODO: have ApiClients share an HTTP client, or one per each is ok?

  static long secondsToWaitForPoolShutdown = 60;

  TestRunner(TestConfiguration config) {
    this.config = config;
    this.scripts = new ArrayList<>();
    this.threadPools = new ArrayList<>();
    this.userJourneyFutureLists = new ArrayList<>();
    this.userJourneyResults = new ArrayList<>();
    this.apiClientsForUsers = new HashMap<>();
  }

  void executeTestConfiguration() throws Exception {
    // specify any value overrides in the Helm chart, then deploy
    if (!config.server.skipDeployment) {
      // modifyHelmValuesAndDeploy();
    }

    // update any Kubernetes properties specified by the test configuration
    if (!config.server.skipKubernetes) {
      KubernetesClientUtils.buildKubernetesClientObject(config.server);
      modifyKubernetesPostDeployment();
    }

    // get an instance of the API client per test user
    for (TestUserSpecification testUser : config.testUsers) {
      ApiClient apiClient = new ApiClient();
      apiClient.setBasePath(config.server.uri);
      GoogleCredentials userCredential = AuthenticationUtils.getDelegatedUserCredential(testUser);
      AccessToken userAccessToken = AuthenticationUtils.getAccessToken(userCredential);
      apiClient.setAccessToken(userAccessToken.getTokenValue());

      apiClientsForUsers.put(testUser.name, apiClient);
    }

    // get an instance of each test script class
    for (TestScriptSpecification testScriptSpecification : config.testScripts) {
      try {
        TestScript testScriptInstance = testScriptSpecification.scriptClass.newInstance();

        // set the billing account for the test script to use
        testScriptInstance.setBillingAccount(config.billingAccount);

        // set any parameters specified by the configuration
        testScriptInstance.setParameters(testScriptSpecification.parameters);

        scripts.add(testScriptInstance);
      } catch (IllegalAccessException | InstantiationException niEx) {
        throw new IllegalArgumentException(
            "Error calling constructor of TestScript class: "
                + testScriptSpecification.scriptClass.getName(),
            niEx);
      }
    }

    // call the setup method of each test script
    Exception setupExceptionThrown = callTestScriptSetups();
    if (setupExceptionThrown != null) {
      callTestScriptCleanups(); // ignore any exceptions thrown by cleanup methods
      throw new RuntimeException("Error calling test script setup methods.", setupExceptionThrown);
    }

    // for each test script
    List<ApiClient> apiClientList = new ArrayList<>(apiClientsForUsers.values());
    for (int tsCtr = 0; tsCtr < scripts.size(); tsCtr++) {
      TestScript testScript = scripts.get(tsCtr);
      TestScriptSpecification testScriptSpecification = config.testScripts.get(tsCtr);

      // add a description to the user journey threads/results that includes any parameters
      String userJourneyDescription = testScriptSpecification.name;
      if (testScriptSpecification.parameters != null) {
        userJourneyDescription += ": " + String.join(",", testScriptSpecification.parameters);
      }

      // create a thread pool for running its user journeys
      ThreadPoolExecutor threadPool =
          (ThreadPoolExecutor)
              Executors.newFixedThreadPool(testScriptSpecification.numberToRunInParallel);
      threadPools.add(threadPool);

      // kick off the user journey(s), one per thread
      List<UserJourneyThread> userJourneyThreads = new ArrayList<>();
      for (int ujCtr = 0; ujCtr < testScriptSpecification.totalNumberToRun; ujCtr++) {
        ApiClient apiClient = apiClientList.get(ujCtr % apiClientList.size());
        userJourneyThreads.add(
            new UserJourneyThread(testScript, userJourneyDescription, apiClient));
      }

      // TODO: support different patterns of kicking off user journeys. here they're all queued at
      // once
      List<Future<UserJourneyResult>> userJourneyFutures = threadPool.invokeAll(userJourneyThreads);
      userJourneyFutureLists.add(userJourneyFutures);
    }

    // wait until all threads either finish or time out
    for (int ctr = 0; ctr < scripts.size(); ctr++) {
      TestScriptSpecification testScriptSpecification = config.testScripts.get(ctr);
      ThreadPoolExecutor threadPool = threadPools.get(ctr);

      threadPool.shutdown();
      long totalTerminationTime =
          testScriptSpecification.expectedTimeForEach * testScriptSpecification.totalNumberToRun;
      boolean terminatedByItself =
          threadPool.awaitTermination(
              totalTerminationTime, testScriptSpecification.expectedTimeForEachUnitObj);

      // if the threads didn't finish in the expected time, then send them interrupts
      if (!terminatedByItself) {
        threadPool.shutdownNow();
      }
      if (!threadPool.awaitTermination(secondsToWaitForPoolShutdown, TimeUnit.SECONDS)) {
        System.out.println(
            "Thread pool for test script "
                + ctr
                + " ("
                + testScriptSpecification.name
                + ") failed to terminate.");
      }
    }

    // compile the results from all thread pools
    for (int ctr = 0; ctr < scripts.size(); ctr++) {
      List<Future<UserJourneyResult>> userJourneyFutureList = userJourneyFutureLists.get(ctr);
      TestScriptSpecification testScriptSpecification = config.testScripts.get(ctr);

      for (Future<UserJourneyResult> userJourneyFuture : userJourneyFutureList) {
        UserJourneyResult result = null;
        if (userJourneyFuture.isDone())
          try {
            // user journey thread completed and populated its own return object, which may include
            // an exception
            result = userJourneyFuture.get();
            result.completed = true;
          } catch (ExecutionException execEx) {
            // user journey thread threw an exception and didn't populate its own return object
            result = new UserJourneyResult(testScriptSpecification.name, "");
            result.completed = false;
            result.exceptionThrown = execEx;
          }
        else {
          // user journey either was never started or got cancelled before it finished
          result = new UserJourneyResult(testScriptSpecification.name, "");
          result.completed = false;
        }
        userJourneyResults.add(result);
      }
    }

    // call the cleanup method of each test script
    Exception cleanupExceptionThrown = callTestScriptCleanups();
    if (cleanupExceptionThrown != null) {
      throw new RuntimeException(
          "Error calling test script cleanup methods.", cleanupExceptionThrown);
    }

    // delete the deployment and restore any Kubernetes settings
    if (!config.server.skipDeployment) {
      deleteDeployment();
    }
    if (!config.server.skipKubernetes) {
      restoreKubernetesSettings();
    }

    // cleanup data project
    cleanupLeftoverTestData();
  }

  /**
   * Call the setup() method of each TestScript class. If one of the classes throws an exception,
   * stop looping through the remaining setup methods and return the exception.
   *
   * @return the exception thrown, null if none
   */
  Exception callTestScriptSetups() {
    for (TestScript testScript : scripts) {
      try {
        testScript.setup(apiClientsForUsers);
      } catch (Exception setupEx) {
        // return the first exception thrown and stop looping through the setup methods
        return setupEx;
      }
    }
    return null;
  }

  /**
   * Call the cleanup() method of each TestScript class. If any of the classes throws an exception,
   * keep looping through the remaining cleanup methods before returning. Save the first exception
   * thrown and return it.
   *
   * @return the first exception thrown, null if none
   */
  Exception callTestScriptCleanups() {
    Exception exceptionThrown = null;
    for (TestScript testScript : scripts) {
      try {
        testScript.cleanup(apiClientsForUsers);
      } catch (Exception cleanupEx) {
        // save the first exception thrown, keep looping through the remaining cleanup methods
        // before returning
        if (exceptionThrown == null) {
          exceptionThrown = cleanupEx;
        }
      }
    }
    return exceptionThrown;
  }

  static class UserJourneyThread implements Callable<UserJourneyResult> {
    TestScript testScript;
    String userJourneyDescription;
    ApiClient apiClient;

    public UserJourneyThread(
        TestScript testScript, String userJourneyDescription, ApiClient apiClient) {
      this.testScript = testScript;
      this.userJourneyDescription = userJourneyDescription;
      this.apiClient = apiClient;
    }

    public UserJourneyResult call() {
      UserJourneyResult result =
          new UserJourneyResult(userJourneyDescription, Thread.currentThread().getName());

      long startTime = System.nanoTime();
      try {
        testScript.userJourney(apiClient);
      } catch (Exception ex) {
        result.exceptionThrown = ex;
      }
      result.elapsedTime = System.nanoTime() - startTime;

      return result;
    }
  }

  void modifyHelmValuesAndDeploy() throws IOException {
    // get INPUT file handle to API deployment Helm YAML file
    File inputFile = new File(config.server.helmApiDeploymentFilePath);
    if (!inputFile.exists()) {
      throw new RuntimeException(
          "API deployment Helm YAML input file does not exist: " + inputFile.getAbsolutePath());
    }

    // get OUTPUT file handle to modified API deployment Helm YAML file
    String outputFilename = inputFile.getName().replaceAll("(?i)\\.yaml", "_TESTRUNNER.yaml");
    File outputFile = new File(inputFile.getParent() + "/" + outputFilename);
    if (outputFile.exists()) {
      boolean deleteSucceeded = outputFile.delete();
      if (!deleteSucceeded) {
        throw new RuntimeException(
            "API deployment Helm YAML existing output file delete failed: "
                + outputFile.getAbsolutePath());
      }
    }
    boolean createSucceeded = outputFile.createNewFile();
    if (!createSucceeded || !outputFile.exists()) {
      throw new RuntimeException(
          "API deployment Helm YAML output file was not created successfully: "
              + outputFile.getAbsolutePath());
    }

    // the file line-by-line parsing below can be brittle because the YAML format is strictly
    // enforced
    BufferedReader reader =
        new BufferedReader(
            new InputStreamReader(new FileInputStream(inputFile), Charset.defaultCharset()));
    BufferedWriter writer =
        new BufferedWriter(
            new OutputStreamWriter(new FileOutputStream(outputFile), Charset.defaultCharset()));

    // loop through the file looking for the "datarepo-api" label, followed by the "env" sub-label
    // echo each line out verbatim to the output file
    boolean lookingForNextEnvSection = false; // true once we've found the "datarepo-api" section
    String line;
    while ((line = reader.readLine()) != null) {
      // echo the line to the output file
      writer.write(line);
      writer.newLine();

      if (!lookingForNextEnvSection) {
        // if we have not already found the "datarepo-api" label, then check the current line
        lookingForNextEnvSection = line.equals("datarepo-api:");
        continue;
      } else if (line.equals("  " + "env:")) {
        // if we have already found the "datarepo-api" lavel, then check the current line for the
        // "env" sub-label
        break;
      }
    }

    // at this point, we're in the env section
    // read in all values under this section, without echoing them back out immediately
    // build a map and pass it to a helper method to process and fill in
    Map<String, String> envVars = new HashMap<>();
    Pattern envVarPattern = Pattern.compile("^\\s+(.*?):\\s(.*)");
    while ((line = reader.readLine()) != null) {
      if (!line.startsWith("    ")) {
        // end of env section
        envVars = processEnvVars(envVars);

        // write the updated map to the output file
        for (Map.Entry<String, String> envVarEntry : envVars.entrySet()) {
          writer.write("    " + envVarEntry.getKey() + ": " + envVarEntry.getValue());
          writer.newLine();
        }

        writer.write(line);
        writer.newLine();
        break;
      }

      Matcher lineMatcher = envVarPattern.matcher(line);
      if (!lineMatcher.find()) {
        reader.close();
        writer.close();
        throw new RuntimeException(
            "Error parsing the datarepo-api/env section of the Helm YAML file: " + line);
      }

      // store the env var name -> value (e.g. GOOGLE_PROJECTID -> broad-jade-dev) in the map
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

    // TODO: call out to helm in a separate process to do the umbrella chart upgrade
    // helm namespace upgrade mm-jade datarepo-helm/datarepo --version=0.1.12 --install --namespace
    // mm -f mmDeployment.yaml
  }

  Map<String, String> processEnvVars(Map<String, String> envVars) {
    // these environment variables are environment-specific and must be defined in the YAML already
    List<String> varsThatMustBeDefined = new ArrayList<String>();
    varsThatMustBeDefined.add("GOOGLE_PROJECTID");
    varsThatMustBeDefined.add("GOOGLE_SINGLEDATAPROJECTID");
    varsThatMustBeDefined.add("DB_DATAREPO_USERNAME");
    varsThatMustBeDefined.add("DB_STAIRWAY_USERNAME");
    varsThatMustBeDefined.add("DB_DATAREPO_URI");
    varsThatMustBeDefined.add("DB_STAIRWAY_URI");
    varsThatMustBeDefined.add("SPRING_PROFILES_ACTIVE");

    Map<String, String> modifiedEnvVars = new HashMap<>();
    for (String var : varsThatMustBeDefined) {
      String varValue = envVars.get(var);
      if (varValue == null) {
        throw new RuntimeException("Expected environment variable not found: " + var);
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

    // set the following environment variables from the config.application specification object
    modifiedEnvVars.put(
        "DATAREPO_MAXSTAIRWAYTHREADS", String.valueOf(config.application.maxStairwayThreads));
    modifiedEnvVars.put(
        "DATAREPO_MAXBULKFILELOAD", String.valueOf(config.application.maxBulkFileLoad));
    modifiedEnvVars.put(
        "DATAREPO_LOADCONCURRENTFILES", String.valueOf(config.application.loadConcurrentFiles));
    modifiedEnvVars.put(
        "DATAREPO_LOADCONCURRENTINGESTS", String.valueOf(config.application.loadConcurrentIngests));
    modifiedEnvVars.put("DATAREPO_INKUBERNETES", String.valueOf(config.application.inKubernetes));
    modifiedEnvVars.put(
        "DATAREPO_LOADHISTORYCOPYCHUNKSIZE",
        String.valueOf(config.application.loadHistoryCopyChunkSize));
    modifiedEnvVars.put(
        "DATAREPO_LOADHISTORYWAITSECONDS",
        String.valueOf(config.application.loadHistoryWaitSeconds));

    return modifiedEnvVars;
  }

  void modifyKubernetesPostDeployment() throws Exception {
    // set the initial number of pods in the API deployment replica set
    V1Deployment apiDeployment = KubernetesClientUtils.getApiDeployment();
    if (apiDeployment == null) {
      throw new RuntimeException("API deployment not found.");
    }
    System.out.println(
        "pod count before set initial replica set size: "
            + KubernetesClientUtils.listPods(config.server.namespace).size());
    apiDeployment =
        KubernetesClientUtils.changeReplicaSetSize(
            apiDeployment, config.kubernetes.numberOfInitialPods);
    KubernetesClientUtils.waitForReplicaSetSizeChange(
        apiDeployment, config.kubernetes.numberOfInitialPods);

    // print out the current pods
    List<V1Pod> pods = KubernetesClientUtils.listPods(config.server.namespace);
    System.out.println("initial number of pods: " + pods.size());
    for (V1Pod pod : pods) {
      System.out.println("  pod: " + pod.getMetadata().getName());
    }
  }

  void deleteDeployment() {
    // TODO: delete DR Manager deployment
  }

  void restoreKubernetesSettings() {
    // TODO: restore Kubernetes settings (where to get the default values -- save from before, or
    // some other place?)
  }

  void cleanupLeftoverTestData() {
    // TODO: cleanup any cloud resources/permissions generated by the test
    // no need to cleanup any DR Manager metadata because each test run re-deploys with a clean
    // database, but reporting that it was left hanging around would be helpful
  }

  void calculateResultStatistics() {
    // TODO: calculate mean/median response time for all completed user journey threads, group by
    // userjourney description
  }

  static void printHelp() {
    System.out.println("Specify test configuration file as first argument.");
    System.out.println("  e.g. ./gradlew :run --args=\"BasicUnauthenticated.json\"");
    System.out.println();
    System.out.println("The following test configuration files were found:");

    // print out the available test configuration found in the resources directory
    List<String> availableTestConfigs =
        FileUtils.getResourcesInDirectory(TestConfiguration.resourceDirectory + "/");
    for (String testConfigFileName : availableTestConfigs) {
      System.out.println("  " + testConfigFileName);
    }
  }

  public static void main(String[] args) throws Exception {
    // if no args specified, print help
    if (args.length < 1) {
      printHelp();
      return;
    }

    // read in configuration, validate it, and print to stdout
    TestConfiguration testConfiguration = TestConfiguration.fromJSONFile(args[0]);
    testConfiguration.validate();
    testConfiguration.display();

    // get an instance of a runner and tell it to execute the configuration
    TestRunner runner = new TestRunner(testConfiguration);
    Exception runnerEx = null;
    try {
      runner.executeTestConfiguration();
    } catch (Exception ex) {
      runnerEx = ex; // save exception to display after printing the results
    }

    // print the results to stdout
    for (UserJourneyResult result : runner.userJourneyResults) {
      result.display();
    }
    if (runnerEx != null) {
      runnerEx.printStackTrace(System.out);
    }

    // calculate any relevant statistics about the user journeys and print them out
    runner.calculateResultStatistics();
  }
}

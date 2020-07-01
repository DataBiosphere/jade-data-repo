package runner;

import bio.terra.datarepo.client.ApiClient;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import io.kubernetes.client.openapi.models.V1Pod;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import runner.config.TestConfiguration;
import runner.config.TestScriptSpecification;
import runner.config.TestUserSpecification;
import utils.AuthenticationUtils;
import utils.KubernetesClientUtils;

class TestRunner {

  TestConfiguration config;
  List<TestScript> scripts;
  List<ThreadPoolExecutor> threadPools;
  List<List<Future<UserJourneyResult>>> userJourneyFutureLists;
  Map<String, ApiClient> apiClientsForUsers; // testUser -> apiClient
  // TODO: have ApiClients share an HTTP client, or one per each is ok?

  static long secondsToWaitForPoolShutdown = 60;

  TestRunner(TestConfiguration config) {
    this.config = config;
    this.scripts = new ArrayList<>();
    this.threadPools = new ArrayList<>();
    this.userJourneyFutureLists = new ArrayList<>();
    this.apiClientsForUsers = new HashMap<>();
  }

  List<UserJourneyResult> executeTestConfiguration() throws Exception {
    // specify any value overrides in the Helm chart, then deploy
    modifyHelmValuesAndDeploy();

    // update any Kubernetes properties specified by the test configuration
    KubernetesClientUtils.buildKubernetesClientObject(config.server);
    modifyKubernetesPostDeployment();

    // get an instance of each test script class
    for (TestScriptSpecification testScriptSpecification : config.testScripts) {
      try {
        // TODO: allow TestScript constructors that take arguments
        TestScript testScriptInstance = testScriptSpecification.scriptClass.newInstance();

        // set the billing account for the test script to use
        testScriptInstance.setBillingAccount(config.billingAccount);

        scripts.add(testScriptInstance);
      } catch (IllegalAccessException | InstantiationException niEx) {
        throw new IllegalArgumentException(
            "Error calling constructor of TestScript class: "
                + testScriptSpecification.scriptClass.getName(),
            niEx);
      }
    }

    // get an instance of the API client per test user
    List<String> userScopes = Arrays.asList("openid", "email", "profile");
    for (TestUserSpecification testUser : config.testUsers) {
      ApiClient apiClient = new ApiClient();
      apiClient.setBasePath(config.server.uri);
      GoogleCredentials userCredential =
          AuthenticationUtils.getDelegatedUserCredential(testUser, userScopes);
      AccessToken userAccessToken = AuthenticationUtils.getAccessToken(userCredential);
      apiClient.setAccessToken(userAccessToken.getTokenValue());

      apiClientsForUsers.put(testUser.name, apiClient);
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
            new UserJourneyThread(testScript, testScriptSpecification.name, apiClient));
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

    // call the cleanup method of each test script
    Exception cleanupExceptionThrown = callTestScriptCleanups();
    if (cleanupExceptionThrown != null) {
      throw new RuntimeException(
          "Error calling test script cleanup methods.", cleanupExceptionThrown);
    }

    // delete the deployment and restore any Kubernetes settings
    teardownClusterAndDeployment();

    // cleanup data project
    cleanupTestData();

    // compile and return the results from all thread pools
    List<UserJourneyResult> results = new ArrayList<>();
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
        results.add(result);
      }
    }
    return results;
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
    String testScriptName;
    ApiClient apiClient;

    public UserJourneyThread(TestScript testScript, String testScriptName, ApiClient apiClient) {
      this.testScript = testScript;
      this.testScriptName = testScriptName;
      this.apiClient = apiClient;
    }

    public UserJourneyResult call() {
      UserJourneyResult result =
          new UserJourneyResult(testScriptName, Thread.currentThread().getName());

      try {
        testScript.userJourney(apiClient);
      } catch (Exception ex) {
        result.exceptionThrown = ex;
      }
      return result;
    }
  }

  void modifyHelmValuesAndDeploy() {
    // TODO: modify Helm template with DR Manager property overrides and do deploy
  }

  void modifyKubernetesPostDeployment() throws Exception {
    // TODO: modify Kubernetes settings post-deployment

    // the below code just lists the existing pods in the cluster, as an example of how to fetch and
    // call the Kubernetes client object
    for (V1Pod item : KubernetesClientUtils.listKubernetesPods()) {
      System.out.println(item.getMetadata().getName());
    }
  }

  void teardownClusterAndDeployment() {
    // TODO: delete DR Manager deployment, restore any Kubernetes settings
  }

  void cleanupTestData() {
    // TODO: cleanup any cloud resources/permissions generated by the test
    // no need to cleanup any DR Manager metadata because each test run re-deploys with a clean
    // database
  }

  static void printHelp() {
    System.out.println("Specify test configuration file as first argument.");
    System.out.println("  e.g. ../gradlew :run --args=\"BasicUnauthenticated.json\"");
  }

  public static void main(String[] args) throws Exception {
    // if no args specified, print help
    if (args.length < 1) {
      printHelp();
      // TODO: also print list of available test configurations
      return;
    }

    // read in configuration, validate it, and print to stdout
    TestConfiguration testConfiguration = TestConfiguration.fromJSONFile(args[0]);
    testConfiguration.validate();
    testConfiguration.display();

    // get an instance of a runner and tell it to execute the configuration
    TestRunner runner = new TestRunner(testConfiguration);
    List<UserJourneyResult> results = runner.executeTestConfiguration();

    // print the results to stdout
    for (UserJourneyResult result : results) {
      result.display();
    }
  }
}

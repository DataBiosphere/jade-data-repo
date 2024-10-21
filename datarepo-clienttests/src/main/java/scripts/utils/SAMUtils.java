package scripts.utils;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import common.utils.AuthenticationUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.broadinstitute.dsde.workbench.client.sam.ApiClient;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.broadinstitute.dsde.workbench.client.sam.api.ResourcesApi;
import org.broadinstitute.dsde.workbench.client.sam.api.VersionApi;
import org.broadinstitute.dsde.workbench.client.sam.model.SamVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import runner.config.ServerSpecification;
import runner.config.TestUserSpecification;

public class SAMUtils {
  private static final Logger logger = LoggerFactory.getLogger(SAMUtils.class);

  private SAMUtils() {}

  private static Map<TestUserSpecification, ApiClient> apiClientsForTestUsers = new HashMap<>();

  /**
   * Build the SAM API client object for the given test user and server specifications. This class
   * maintains a cache of API client objects, and will return the cached object if it already
   * exists. The token is always refreshed, regardless of whether the API client object was found in
   * the cache or not.
   *
   * @param testUser the test user whose credentials are supplied to the API client object
   * @param server the server we are testing against
   * @return the API client object for this user
   */
  public static ApiClient getClientForTestUser(
      TestUserSpecification testUser, ServerSpecification server) throws IOException {
    // refresh the user token
    GoogleCredentials userCredential = AuthenticationUtils.getDelegatedUserCredential(testUser);
    AccessToken userAccessToken = AuthenticationUtils.getAccessToken(userCredential);

    // first check if there is already a cached ApiClient for this test user
    ApiClient cachedApiClient = apiClientsForTestUsers.get(testUser);
    if (cachedApiClient != null) {
      // refresh the token here before returning
      // this should be helpful for long-running tests (roughly > 1hr)
      cachedApiClient.setAccessToken(userAccessToken.getTokenValue());

      return cachedApiClient;
    }

    // TODO: have ApiClients share an HTTP client, or one per each is ok?
    // no cached ApiClient found, so build a new one here and add it to the cache before returning
    logger.debug(
        "Fetching credentials and building SAM ApiClient object for test user: {}", testUser.name);
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(server.samUri);
    apiClient.setUserAgent("OpenAPI-Generator/1.0.0 java"); // only logs an error in sam

    apiClient.setAccessToken(userAccessToken.getTokenValue());

    apiClientsForTestUsers.put(testUser, apiClient);
    return apiClient;
  }

  /**
   * Call the SAM endpoint to get the version of the server that is currently running.
   *
   * @return the SAM version object
   */
  public static SamVersion getVersion(ApiClient apiClient) throws ApiException {
    VersionApi versionApi = new VersionApi(apiClient);
    return versionApi.samVersion();
  }

  /**
   * Call the SAM endpoint to check if the caller is a Data Repo steward.
   *
   * @return true if the caller is a steward, false otherwise
   */
  public static boolean isDataRepoSteward(ApiClient apiClient, String datarepoResourceId)
      throws ApiException {
    ResourcesApi resourcesApi = new ResourcesApi(apiClient);
    return resourcesApi.resourcePermissionV2("datarepo", datarepoResourceId, "create_dataset");
  }

  /**
   * Call the SAM endpoint to check if the caller is a Data Repo Admin.
   *
   * @return true if the caller is a admin, false otherwise
   */
  public static boolean isDataRepoAdmin(ApiClient apiClient, String datarepoResourceId)
      throws ApiException {
    ResourcesApi resourcesApi = new ResourcesApi(apiClient);
    return resourcesApi.resourcePermissionV2("datarepo", datarepoResourceId, "configure");
  }

  /**
   * Call the SAM endpoint to check if the provided test user is a steward
   *
   * @param testUser test user to check
   * @param server server to check against
   * @return true if the test user is a steward
   * @throws Exception from underlying code
   */
  public static boolean isDataRepoSteward(
      TestUserSpecification testUser, ServerSpecification server) throws Exception {
    ApiClient apiClient = getClientForTestUser(testUser, server);
    return isDataRepoSteward(apiClient, server.samResourceIdForDatarepo);
  }

  /**
   * Returns a random test user from the given list that is a Data Repo steward, null if none found.
   *
   * @param testUsers the list of test users to check
   * @param server the server we are testing against
   * @return a test user that is a steward, null if none found
   */
  public static TestUserSpecification findTestUserThatIsDataRepoSteward(
      List<TestUserSpecification> testUsers, ServerSpecification server) throws Exception {
    // create a copy of the list and randomly reorder it
    List<TestUserSpecification> testUsersCopy = new ArrayList<>(testUsers);
    Collections.shuffle(testUsersCopy);

    // iterate through the list copy, return the first test user that is a data repo steward
    for (TestUserSpecification testUser : testUsersCopy) {
      ApiClient apiClient = getClientForTestUser(testUser, server);
      if (isDataRepoSteward(apiClient, server.samResourceIdForDatarepo)) {
        return testUser;
      }
    }
    return null;
  }

  public static TestUserSpecification findTestUserThatIsDataRepoAdmin(
      List<TestUserSpecification> testUsers, ServerSpecification server) throws Exception {
    // create a copy of the list and randomly reorder it
    List<TestUserSpecification> testUsersCopy = new ArrayList<>(testUsers);
    Collections.shuffle(testUsersCopy);

    // iterate through the list copy, return the first test user that is a data repo admin
    for (TestUserSpecification testUser : testUsersCopy) {
      ApiClient apiClient = getClientForTestUser(testUser, server);
      if (isDataRepoAdmin(apiClient, server.samResourceIdForDatarepo)) {
        return testUser;
      }
    }
    return null;
  }
}

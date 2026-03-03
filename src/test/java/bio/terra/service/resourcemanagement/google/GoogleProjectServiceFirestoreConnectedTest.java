package bio.terra.service.resourcemanagement.google;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.app.model.GoogleRegion;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Connected;
import bio.terra.common.fixtures.ConnectedOperations;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.filedata.google.firestore.FireStoreProject;
import bio.terra.service.resourcemanagement.BufferService;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.appengine.v1.Appengine;
import com.google.api.services.appengine.v1.model.Application;
import com.google.cloud.firestore.CollectionReference;
import com.google.cloud.firestore.DocumentReference;
import com.google.cloud.firestore.DocumentSnapshot;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.v1.FirestoreAdminClient;
import com.google.firestore.admin.v1.Database;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class GoogleProjectServiceFirestoreConnectedTest {
  private static final Logger logger =
      LoggerFactory.getLogger(GoogleProjectServiceFirestoreConnectedTest.class);

  @Autowired private BufferService bufferService;
  @Autowired private GoogleResourceManagerService resourceManagerService;
  @Autowired private ConnectedOperations connectedOperations;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private GoogleResourceConfiguration resourceConfiguration;
  @MockitoBean private IamProviderInterface samService;

  private BillingProfileModel billingProfile;
  private String testProjectId;
  private GoogleRegion testRegion = GoogleRegion.US_CENTRAL1;

  @Before
  public void setup() throws Exception {
    connectedOperations.stubOutSamCalls(samService);
    billingProfile =
        connectedOperations.createProfileForAccount(testConfig.getGoogleBillingAccountId());

    // Get a project from RBS
    ResourceInfo resource = bufferService.handoutResource(false);
    testProjectId = resource.getCloudResourceUid().getGoogleProjectUid().getProjectId();

    // Enable Firestore using App Engine API (prerequisite for database creation)
    // This is what enableFirestore() does before calling createFirestoreDefaultDatabase
    enableFirestoreApp(testProjectId, testRegion);
  }

  /**
   * Enable Firestore by creating an App Engine application. This is extracted from
   * GoogleProjectService.enableFirestore() to serve as the prerequisite setup before testing
   * createFirestoreDefaultDatabase directly.
   */
  private void enableFirestoreApp(String projectId, GoogleRegion region) throws Exception {
    Appengine appengine = createAppengineClient();
    GoogleRegion firestoreRegion = region.getRegionOrFallbackFirestoreRegion();

    logger.info("Creating App Engine application in project {} at {}", projectId, firestoreRegion);

    Application app =
        new Application()
            .setId(projectId)
            .setLocationId(firestoreRegion.toString())
            .setDatabaseType("CLOUD_FIRESTORE");

    Appengine.Apps.Create createRequest = appengine.apps().create(app);
    createRequest.getRequestHeaders().set("X-Goog-User-Project", projectId);

    com.google.api.services.appengine.v1.model.Operation operation = createRequest.execute();

    // Wait for App Engine application creation to complete
    long timeout = resourceConfiguration.projectCreateTimeoutSeconds();
    waitForAppengineOperation(appengine, operation, projectId, timeout);
    logger.info("App Engine application created successfully");
  }

  private Appengine createAppengineClient() throws Exception {
    HttpTransport httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    JsonFactory jsonFactory = JacksonFactory.getDefaultInstance();

    GoogleCredential credential = GoogleCredential.getApplicationDefault();
    if (credential.createScopedRequired()) {
      credential =
          credential.createScoped(
              Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    }

    return new Appengine.Builder(httpTransport, jsonFactory, credential)
        .setApplicationName(resourceConfiguration.applicationName())
        .build();
  }

  private void waitForAppengineOperation(
      Appengine appengine,
      com.google.api.services.appengine.v1.model.Operation operation,
      String appId,
      long timeoutSeconds)
      throws Exception {
    long start = System.currentTimeMillis();
    final long pollInterval = TimeUnit.SECONDS.toMillis(10);
    final String opName = operation.getName();
    final String opId = GoogleProjectService.extractOperationIdFromName(appId, opName);

    while (operation != null && (operation.getDone() == null || !operation.getDone())) {
      com.google.api.services.appengine.v1.model.Status error = operation.getError();
      if (error != null) {
        throw new RuntimeException("Error while waiting for operation: " + error.getMessage());
      }
      Thread.sleep(pollInterval);
      long elapsed = System.currentTimeMillis() - start;
      if (elapsed >= TimeUnit.SECONDS.toMillis(timeoutSeconds)) {
        throw new RuntimeException("Timed out waiting for operation to complete");
      }
      logger.info("Checking App Engine operation: {}", opId);
      Appengine.Apps.Operations.Get request = appengine.apps().operations().get(appId, opId);
      request.getRequestHeaders().set("X-Goog-User-Project", appId);
      operation = request.execute();
    }
  }

  @Test
  public void testCheckFirestoreDatabaseExists() throws Exception {
    // Use a custom database name to ensure it doesn't exist initially
    // (the default database may already exist from App Engine setup)
    String testDatabaseId = "test-existence-check-db";

    try (FirestoreAdminClient firestoreAdminClient = FirestoreAdminClient.create()) {
      // Before creating, custom database should not exist
      boolean existsBefore =
          GoogleProjectService.checkFirestoreDatabaseExists(
              firestoreAdminClient, testProjectId, testDatabaseId);
      assertThat("Database does not exist initially", existsBefore, is(false));

      // Create the database
      GoogleProjectService.createFirestoreDatabase(
          firestoreAdminClient,
          testProjectId,
          testRegion.getRegionOrFallbackFirestoreRegion().toString(),
          testDatabaseId);

      // After creating, database should exist
      boolean existsAfter =
          GoogleProjectService.checkFirestoreDatabaseExists(
              firestoreAdminClient, testProjectId, testDatabaseId);
      assertThat("Database exists after creation", existsAfter, is(true));

      logger.info("Successfully verified database existence check for {}", testDatabaseId);
    }
  }

  @Test
  public void testCreateFirestoreDatabase() throws Exception {
    String customDatabaseId = "test-custom-db";
    try (FirestoreAdminClient firestoreAdminClient = FirestoreAdminClient.create()) {
      // Create a custom database
      Database createdDatabase =
          GoogleProjectService.createFirestoreDatabase(
              firestoreAdminClient,
              testProjectId,
              testRegion.getRegionOrFallbackFirestoreRegion().toString(),
              customDatabaseId);

      assertThat("Created database is not null", createdDatabase, is(notNullValue()));
      assertThat(
          "Database name contains custom ID",
          createdDatabase.getName(),
          containsString(customDatabaseId));
      assertThat(
          "Database type is FIRESTORE_NATIVE",
          createdDatabase.getType(),
          is(Database.DatabaseType.FIRESTORE_NATIVE));
      assertThat(
          "Database location is correct",
          createdDatabase.getLocationId(),
          is(testRegion.getRegionOrFallbackFirestoreRegion().toString()));

      // Verify it exists using the check method
      boolean exists =
          GoogleProjectService.checkFirestoreDatabaseExists(
              firestoreAdminClient, testProjectId, customDatabaseId);
      assertThat("Custom database exists", exists, is(true));

      logger.info("Successfully created and verified custom database: {}", customDatabaseId);
    }

    // Verify we can actually use the custom database to write/read data
    // Note: We need to explicitly connect to the named database (not the default one)
    com.google.cloud.firestore.FirestoreOptions firestoreOptions =
        com.google.cloud.firestore.FirestoreOptions.newBuilder()
            .setProjectId(testProjectId)
            .setDatabaseId(customDatabaseId)
            .build();
    Firestore customFirestore = firestoreOptions.getService();

    try {
      CollectionReference testCollection = customFirestore.collection("custom-db-collection");
      DocumentReference docRef = testCollection.document("custom-db-doc");

      Map<String, Object> testData =
          Map.of(
              "test-field",
              "custom-db-value",
              "database-id",
              customDatabaseId,
              "timestamp",
              System.currentTimeMillis());
      docRef.set(testData).get(); // Write

      DocumentSnapshot snapshot = docRef.get().get(); // Read
      assertThat("Can read from custom database", snapshot.exists(), is(true));
      assertThat("Data is correct", snapshot.getString("test-field"), is("custom-db-value"));
      assertThat(
          "Database ID field is correct", snapshot.getString("database-id"), is(customDatabaseId));

      logger.info(
          "Successfully verified custom database {} is usable for read/write operations",
          customDatabaseId);
    } finally {
      customFirestore.close();
    }
  }

  @Test
  public void testCreateFirestoreDefaultDatabase() throws Exception {
    // Call the method directly to create the Firestore database
    GoogleProjectService.createFirestoreDefaultDatabase(
        testProjectId, testRegion.getRegionOrFallbackFirestoreRegion().toString());

    // Verify the Firestore (default) database exists using FirestoreAdminClient
    try (FirestoreAdminClient firestoreAdminClient = FirestoreAdminClient.create()) {
      String databaseName = String.format("projects/%s/databases/(default)", testProjectId);
      Database database = firestoreAdminClient.getDatabase(databaseName);

      assertThat("Database exists", database, is(notNullValue()));
      assertThat("Database name is correct", database.getName(), containsString("(default)"));
      assertThat(
          "Database type is FIRESTORE_NATIVE",
          database.getType(),
          is(Database.DatabaseType.FIRESTORE_NATIVE));
      assertThat(
          "Database location is correct",
          database.getLocationId(),
          is(testRegion.getRegionOrFallbackFirestoreRegion().toString()));
    }

    // Verify we can actually use Firestore to write/read data
    FireStoreProject firestoreProject = FireStoreProject.get(testProjectId);
    Firestore firestore = firestoreProject.getFirestore();
    CollectionReference testCollection = firestore.collection("test-collection");
    DocumentReference docRef = testCollection.document("test-doc");

    Map<String, Object> testData =
        Map.of("test-field", "test-value", "timestamp", System.currentTimeMillis());
    docRef.set(testData).get(); // Write

    DocumentSnapshot snapshot = docRef.get().get(); // Read
    assertThat("Can read from Firestore", snapshot.exists(), is(true));
    assertThat("Data is correct", snapshot.getString("test-field"), is("test-value"));

    logger.info("Successfully created and verified Firestore default database");
  }

  @Test
  public void testIdempotentDatabaseCreation() throws Exception {
    // First call - creates database
    GoogleProjectService.createFirestoreDefaultDatabase(
        testProjectId, testRegion.getRegionOrFallbackFirestoreRegion().toString());

    // Second call - should not fail (idempotent check)
    GoogleProjectService.createFirestoreDefaultDatabase(
        testProjectId, testRegion.getRegionOrFallbackFirestoreRegion().toString());

    // Verify database still exists and is functional
    try (FirestoreAdminClient firestoreAdminClient = FirestoreAdminClient.create()) {
      String databaseName = String.format("projects/%s/databases/(default)", testProjectId);
      Database database = firestoreAdminClient.getDatabase(databaseName);
      assertThat("Database still exists after second call", database, is(notNullValue()));
    }

    // Test that Firestore operations still work after idempotent call
    Firestore firestore = FireStoreProject.get(testProjectId).getFirestore();

    // Write new test data
    CollectionReference testCollection = firestore.collection("test-collection-idempotent");
    DocumentReference docRef = testCollection.document("test-doc-idempotent");
    Map<String, Object> testData =
        Map.of("test-field", "idempotent-value", "timestamp", System.currentTimeMillis());
    docRef.set(testData).get();

    // Verify we can read it back
    DocumentSnapshot snapshot = docRef.get().get();
    assertThat("Can still read after idempotent call", snapshot.exists(), is(true));
    assertThat("Data is correct", snapshot.getString("test-field"), is("idempotent-value"));

    logger.info("Successfully verified idempotent database creation");
  }

  @After
  public void teardown() throws Exception {
    // Clean up test data in Firestore if needed
    if (testProjectId != null) {
      try {
        Firestore firestore = FireStoreProject.get(testProjectId).getFirestore();

        // Clean up from first test
        try {
          firestore.collection("test-collection").document("test-doc").delete().get();
        } catch (Exception e) {
          logger.warn("Failed to clean up test-collection/test-doc", e);
        }

        // Clean up from idempotency test
        try {
          firestore
              .collection("test-collection-idempotent")
              .document("test-doc-idempotent")
              .delete()
              .get();
        } catch (Exception e) {
          logger.warn("Failed to clean up test-collection-idempotent/test-doc-idempotent", e);
        }

        // Clean up from testCreateFirestoreDatabase test (custom database)
        try {
          String customDatabaseId = "test-custom-db";
          com.google.cloud.firestore.FirestoreOptions firestoreOptions =
              com.google.cloud.firestore.FirestoreOptions.newBuilder()
                  .setProjectId(testProjectId)
                  .setDatabaseId(customDatabaseId)
                  .build();
          Firestore customFirestore = firestoreOptions.getService();
          try {
            customFirestore
                .collection("custom-db-collection")
                .document("custom-db-doc")
                .delete()
                .get();
            logger.info("Cleaned up custom database test data");
          } finally {
            customFirestore.close();
          }
        } catch (Exception e) {
          logger.warn("Failed to clean up custom-db-collection/custom-db-doc", e);
        }

        // Clean up from testCheckFirestoreDatabaseExists test
        // Note: We don't need to clean up data since this test doesn't write any documents
        // The database itself will remain in the project (returned to RBS pool)
        logger.info(
            "testCheckFirestoreDatabaseExists database cleanup not needed (no documents written)");
      } catch (Exception e) {
        // Ignore cleanup errors - project will be returned to RBS pool
        logger.warn("Failed to clean up Firestore test data", e);
      }
    }
    connectedOperations.teardown();
  }
}

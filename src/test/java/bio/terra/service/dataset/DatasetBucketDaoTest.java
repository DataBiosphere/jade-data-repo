package bio.terra.service.dataset;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.DaoOperations;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
@EmbeddedDatabaseTest
class DatasetBucketDaoTest {
  private static final Logger logger = LoggerFactory.getLogger(DatasetBucketDaoTest.class);

  @Autowired private DatasetDao datasetDao;

  @Autowired private DatasetBucketDao datasetBucketDao;

  @Autowired private ProfileDao profileDao;

  @Autowired private GoogleResourceDao resourceDao;

  @Autowired private DaoOperations daoOperations;

  private BillingProfileModel billingProfile;
  private GoogleProjectResource projectResource;
  private Dataset dataset;
  private UUID datasetId;

  private final List<UUID> billingProfileIds = new ArrayList<>();
  private final List<UUID> datasetIds = new ArrayList<>();
  private final Map<String, String> bucketList = new HashMap<>();
  private final List<UUID> projectIds = new ArrayList<>();
  private final Map<UUID, UUID> datasetIdsToBucketResourceIds = new HashMap<>();

  private String bucketName;

  @BeforeEach
  public void setup() throws IOException {
    BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
    billingProfile = profileDao.createBillingProfile(profileRequest, "testUser");
    billingProfileIds.add(billingProfile.getId());

    projectResource = ResourceFixtures.randomProjectResource(billingProfile);
    UUID projectId = resourceDao.createProject(projectResource);
    projectResource.id(projectId);
    projectIds.add(projectId);

    dataset =
        daoOperations.createDataset(
            billingProfile.getId(), projectId, DaoOperations.DATASET_MINIMAL);
    datasetId = dataset.getId();
    datasetIds.add(datasetId);
  }

  @AfterEach
  public void teardown() {

    datasetIdsToBucketResourceIds.forEach(
        (id, bucketResourceId) -> {
          try {
            datasetBucketDao.deleteDatasetBucketLink(id, bucketResourceId);
          } catch (Exception ex) {
            logger.error(
                "[CLEANUP] Unable to delete dataset bucket link for dataset {} and bucket resource {}",
                id,
                bucketResourceId);
          }
        });
    datasetIds.forEach(
        id -> {
          try {
            datasetDao.delete(id);
          } catch (Exception ex) {
            logger.error("[CLEANUP] Unable to delete dataset {}", id);
          }
        });
    bucketList.forEach(
        (bucket, flightId) -> {
          try {
            resourceDao.deleteBucketMetadata(bucket, flightId);
          } catch (Exception ex) {
            logger.error(
                "[CLEANUP] Unable to bucket metadata for bucket {} and flight {}",
                bucket,
                flightId);
          }
        });
    projectIds.forEach(
        projectId -> {
          try {
            resourceDao.deleteProject(projectId);
          } catch (Exception ex) {
            logger.error("[CLEANUP] Unable to delete entry in database for projects {}", projectId);
          }
        });

    billingProfileIds.forEach(
        billingProfileId -> {
          try {
            profileDao.deleteBillingProfileById(billingProfileId);
          } catch (Exception ex) {
            logger.error("[CLEANUP] Unable to billing profile {}", billingProfileId);
          }
        });
  }

  @Test
  void testGetProjectForDatasetProfileCombo() throws Exception {
    UUID bucketResourceId = createBucketDbEntry(projectResource);
    datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
    datasetIdsToBucketResourceIds.put(datasetId, bucketResourceId);

    String newProjectName =
        datasetBucketDao.getProjectResourceForBucket(datasetId, billingProfile.getId());

    assertEquals(
        projectResource.getGoogleProjectId(), newProjectName, "Should retrieve existing project");

    // Get Project given new billing profile
    BillingProfileRequestModel profileRequest2 = ProfileFixtures.randomBillingProfileRequest();
    BillingProfileModel billingProfile2 =
        profileDao.createBillingProfile(profileRequest2, "testUser");
    billingProfileIds.add(billingProfile2.getId());
    assertNull(
        datasetBucketDao.getProjectResourceForBucket(datasetId, billingProfile2.getId()),
        "Should NOT retrieve existing project");

    List<UUID> projectResourceIds =
        datasetBucketDao.getProjectResourceIdsForBucketPerDataset(datasetId);
    assertEquals(1, projectResourceIds.size(), "Just one bucket linked right now");

    // Link dataset to new billing profile via bucket
    GoogleProjectResource ingestProjectResource =
        ResourceFixtures.randomProjectResource(billingProfile2);
    UUID ingestProjectId = resourceDao.createProject(ingestProjectResource);
    ingestProjectResource.id(ingestProjectId);
    UUID ingestBucketResourceId = createBucketDbEntry(ingestProjectResource);
    datasetBucketDao.createDatasetBucketLink(datasetId, ingestBucketResourceId);
    datasetIdsToBucketResourceIds.put(datasetId, ingestBucketResourceId);

    List<UUID> projectResourceIds_after =
        datasetBucketDao.getProjectResourceIdsForBucketPerDataset(datasetId);
    assertEquals(
        2,
        projectResourceIds_after.size(),
        "Should return both projects for both billing profiles for dataset 1");

    // Get project given a new dataset
    Dataset dataset_second =
        daoOperations.createDataset(
            billingProfile2.getId(), ingestProjectId, DaoOperations.DATASET_MINIMAL);
    datasetIds.add(dataset_second.getId());
    createBucketDbEntry(projectResource);
    assertNull(
        datasetBucketDao.getProjectResourceForBucket(
            dataset_second.getId(), billingProfile.getId()),
        "Should NOT retrieve existing project");
  }

  @Test
  void testDatasetBucketLink() {
    UUID bucketResourceId = createBucketDbEntry(projectResource);

    // initial check - link should not yet exist
    boolean linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertFalse(linkExists, "Link should not yet exist.");

    // create link
    datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
    datasetIdsToBucketResourceIds.put(datasetId, bucketResourceId);
    linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertTrue(linkExists, "Link should now exist.");

    // delete link
    datasetBucketDao.deleteDatasetBucketLink(datasetId, bucketResourceId);
    linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertFalse(linkExists, "Link should no longer exists.");
  }

  @Test
  void testMultipleLinks() {
    UUID bucketResourceId = createBucketDbEntry(projectResource);

    // initial check - link should not yet exist
    boolean linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertFalse(linkExists, "Link should not yet exist.");

    // create link
    datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
    datasetIdsToBucketResourceIds.put(datasetId, bucketResourceId);
    linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertTrue(linkExists, "Link should now exist.");
    int linkCount =
        datasetBucketDao.datasetBucketSuccessfulIngestCount(datasetId, bucketResourceId);
    assertEquals(1, linkCount, "Link count should be 1.");

    // create link
    datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
    datasetIdsToBucketResourceIds.put(datasetId, bucketResourceId);
    linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertTrue(linkExists, "Link should now exist.");
    linkCount = datasetBucketDao.datasetBucketSuccessfulIngestCount(datasetId, bucketResourceId);
    assertEquals(2, linkCount, "Link count should be 2.");

    // delete link
    datasetBucketDao.deleteDatasetBucketLink(datasetId, bucketResourceId);
    linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertFalse(linkExists, "Link should no longer exists.");
  }

  @Test
  void testDecrementLink() {
    UUID bucketResourceId = createBucketDbEntry(projectResource);

    // initial check - link should not yet exist
    boolean linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertFalse(linkExists, "Link should not yet exist.");

    // create link
    datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
    datasetIdsToBucketResourceIds.put(datasetId, bucketResourceId);
    linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertTrue(linkExists, "Link should now exist.");

    // decrement the link
    datasetBucketDao.decrementDatasetBucketLink(datasetId, bucketResourceId);
    int linkCount =
        datasetBucketDao.datasetBucketSuccessfulIngestCount(datasetId, bucketResourceId);
    assertEquals(
        0, linkCount, "After decrementing bucket link, successful ingest count should equal 0.");
  }

  // Test key restraints - There must be entries in the dataset table and bucket_resource table
  // in order to create a link in the dataset_bucket table
  @Test
  void datasetMustExistToLink() {
    // create bucket for dataset
    UUID bucketResourceId = createBucketDbEntry(projectResource);

    // fake datasetId
    UUID randomDatasetId = UUID.randomUUID();

    // initial check - link should not yet exist
    boolean linkExists =
        datasetBucketDao.datasetBucketLinkExists(randomDatasetId, bucketResourceId);
    assertFalse(linkExists, "Link should not yet exist.");

    // this should fail -> no requires real dataset and bucket to link
    assertThrows(
        Exception.class,
        () -> datasetBucketDao.createDatasetBucketLink(randomDatasetId, bucketResourceId));
  }

  @Test
  void bucketMustExistToLink() {
    // fake datasetId
    UUID randomBucketResourceId = UUID.randomUUID();

    // initial check - link should not yet exist
    boolean linkExists =
        datasetBucketDao.datasetBucketLinkExists(datasetId, randomBucketResourceId);
    assertFalse(linkExists, "Link should not yet exist.");

    // this should fail -> no requires real dataset and bucket to link
    assertThrows(
        Exception.class,
        () -> datasetBucketDao.createDatasetBucketLink(datasetId, randomBucketResourceId));
  }

  @Test
  void testAutoclassEnabledFlag() {
    boolean autoclassEnabled = true;
    UUID bucketId = createBucketDbEntry(projectResource, autoclassEnabled);
    GoogleBucketResource bucket = resourceDao.retrieveBucketById(bucketId);
    assertTrue(bucket.getAutoclassEnabled(), "Correct autoclass setting is returned");
    GoogleBucketResource retrievedBucket =
        resourceDao.getBucket(bucketName, projectResource.getId());
    assertTrue(retrievedBucket.getAutoclassEnabled(), "Correct autoclass setting is returned");
  }

  @Test
  void testAutoclassDisabledFlag() {
    boolean autoclassEnabled = false;
    UUID bucketId = createBucketDbEntry(projectResource, autoclassEnabled);
    GoogleBucketResource bucket = resourceDao.retrieveBucketById(bucketId);
    assertFalse(bucket.getAutoclassEnabled(), "Correct autoclass setting is returned");
    GoogleBucketResource retrievedBucket =
        resourceDao.getBucket(bucketName, projectResource.getId());
    assertFalse(retrievedBucket.getAutoclassEnabled(), "Correct autoclass setting is returned");
  }

  private UUID createBucketDbEntry(GoogleProjectResource projectResource2) {
    return createBucketDbEntry(projectResource2, true);
  }

  private UUID createBucketDbEntry(
      GoogleProjectResource projectResource2, boolean autoclassEnabled) {
    String ingestFileFlightId = UUID.randomUUID().toString();
    bucketName = String.format("testbucket%s", ingestFileFlightId);
    GoogleBucketResource bucketResource =
        resourceDao.createAndLockBucket(
            bucketName,
            projectResource2,
            (GoogleRegion)
                dataset.getDatasetSummary().getStorageResourceRegion(GoogleCloudResource.BUCKET),
            ingestFileFlightId,
            autoclassEnabled);
    bucketList.put(bucketResource.getName(), ingestFileFlightId);
    return bucketResource.getResourceId();
  }
}

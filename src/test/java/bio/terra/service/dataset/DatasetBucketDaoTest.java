package bio.terra.service.dataset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

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
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
@EmbeddedDatabaseTest
public class DatasetBucketDaoTest {
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

  private String ingestFileFlightId;
  private String bucketName;

  @Before
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

  @After
  public void teardown() {

    datasetIdsToBucketResourceIds.forEach(
        (datasetId, bucketResourceId) -> {
          try {
            datasetBucketDao.deleteDatasetBucketLink(datasetId, bucketResourceId);
          } catch (Exception ex) {
            logger.error(
                "[CLEANUP] Unable to delete dataset bucket link for dataset {} and bucket resource {}",
                datasetId,
                bucketResourceId);
          }
        });
    datasetIds.forEach(
        datasetId -> {
          try {
            datasetDao.delete(datasetId);
          } catch (Exception ex) {
            logger.error("[CLEANUP] Unable to delete dataset {}", datasetId);
          }
        });
    bucketList.forEach(
        (bucketName, flightId) -> {
          try {
            resourceDao.deleteBucketMetadata(bucketName, flightId);
          } catch (Exception ex) {
            logger.error(
                "[CLEANUP] Unable to bucket metadata for bucket {} and flight {}",
                bucketName,
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
  public void testGetProjectForDatasetProfileCombo() throws Exception {
    UUID bucketResourceId = createBucketDbEntry(projectResource);
    datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
    datasetIdsToBucketResourceIds.put(datasetId, bucketResourceId);

    String newProjectName =
        datasetBucketDao.getProjectResourceForBucket(datasetId, billingProfile.getId());

    assertEquals(
        "Should retrieve existing project", projectResource.getGoogleProjectId(), newProjectName);

    // Get Project given new billing profile
    BillingProfileRequestModel profileRequest2 = ProfileFixtures.randomBillingProfileRequest();
    BillingProfileModel billingProfile2 =
        profileDao.createBillingProfile(profileRequest2, "testUser");
    billingProfileIds.add(billingProfile2.getId());
    assertNull(
        "Should NOT retrieve existing project",
        datasetBucketDao.getProjectResourceForBucket(datasetId, billingProfile2.getId()));

    List<UUID> projectResourceIds =
        datasetBucketDao.getProjectResourceIdsForBucketPerDataset(datasetId);
    assertEquals("Just one bucket linked right now", 1, projectResourceIds.size());

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
        "Should return both projects for both billing profiles for dataset 1",
        2,
        projectResourceIds_after.size());

    // Get project given a new dataset
    Dataset dataset_second =
        daoOperations.createDataset(
            billingProfile2.getId(), ingestProjectId, DaoOperations.DATASET_MINIMAL);
    datasetIds.add(dataset_second.getId());
    createBucketDbEntry(projectResource);
    assertNull(
        "Should NOT retrieve existing project",
        datasetBucketDao.getProjectResourceForBucket(
            dataset_second.getId(), billingProfile.getId()));
  }

  @Test
  public void testDatasetBucketLink() {
    UUID bucketResourceId = createBucketDbEntry(projectResource);

    // initial check - link should not yet exist
    boolean linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertFalse("Link should not yet exist.", linkExists);

    // create link
    datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
    datasetIdsToBucketResourceIds.put(datasetId, bucketResourceId);
    linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertTrue("Link should now exist.", linkExists);

    // delete link
    datasetBucketDao.deleteDatasetBucketLink(datasetId, bucketResourceId);
    linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertFalse("Link should no longer exists.", linkExists);
  }

  @Test
  public void testMultipleLinks() {
    UUID bucketResourceId = createBucketDbEntry(projectResource);

    // initial check - link should not yet exist
    boolean linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertFalse("Link should not yet exist.", linkExists);

    // create link
    datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
    datasetIdsToBucketResourceIds.put(datasetId, bucketResourceId);
    linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertTrue("Link should now exist.", linkExists);
    int linkCount =
        datasetBucketDao.datasetBucketSuccessfulIngestCount(datasetId, bucketResourceId);
    assertEquals("Link count should be 1.", 1, linkCount);

    // create link
    datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
    datasetIdsToBucketResourceIds.put(datasetId, bucketResourceId);
    linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertTrue("Link should now exist.", linkExists);
    linkCount = datasetBucketDao.datasetBucketSuccessfulIngestCount(datasetId, bucketResourceId);
    assertEquals("Link count should be 2.", 2, linkCount);

    // delete link
    datasetBucketDao.deleteDatasetBucketLink(datasetId, bucketResourceId);
    linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertFalse("Link should no longer exists.", linkExists);
  }

  @Test
  public void testDecrementLink() {
    UUID bucketResourceId = createBucketDbEntry(projectResource);

    // initial check - link should not yet exist
    boolean linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertFalse("Link should not yet exist.", linkExists);

    // create link
    datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
    datasetIdsToBucketResourceIds.put(datasetId, bucketResourceId);
    linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertTrue("Link should now exist.", linkExists);

    // decrement the link
    datasetBucketDao.decrementDatasetBucketLink(datasetId, bucketResourceId);
    int linkCount =
        datasetBucketDao.datasetBucketSuccessfulIngestCount(datasetId, bucketResourceId);
    assertEquals(
        "After decrementing bucket link, successful ingest count should equal 0.", 0, linkCount);
  }

  // Test key restraints - There must be entries in the dataset table and bucket_resource table
  // in order to create a link in the dataset_bucket table
  @Test(expected = Exception.class)
  public void datasetMustExistToLink() {
    // create bucket for dataset
    UUID bucketResourceId = createBucketDbEntry(projectResource);

    // fake datasetId
    UUID randomDatasetId = UUID.randomUUID();

    // initial check - link should not yet exist
    boolean linkExists =
        datasetBucketDao.datasetBucketLinkExists(randomDatasetId, bucketResourceId);
    assertFalse("Link should not yet exist.", linkExists);

    // this should fail -> no requires real dataset and bucket to link
    datasetBucketDao.createDatasetBucketLink(randomDatasetId, bucketResourceId);
  }

  @Test(expected = Exception.class)
  public void bucketMustExistToLink() {
    // fake datasetId
    UUID randomBucketResourceId = UUID.randomUUID();

    // initial check - link should not yet exist
    boolean linkExists =
        datasetBucketDao.datasetBucketLinkExists(datasetId, randomBucketResourceId);
    assertFalse("Link should not yet exist.", linkExists);

    // this should fail -> no requires real dataset and bucket to link
    datasetBucketDao.createDatasetBucketLink(datasetId, randomBucketResourceId);
  }

  @Test
  public void testAutoclassEnabledFlag() {
    boolean autoclassEnabled = true;
    UUID bucketId = createBucketDbEntry(projectResource, autoclassEnabled);
    GoogleBucketResource bucket = resourceDao.retrieveBucketById(bucketId);
    assertTrue("Correct autoclass setting is returned", bucket.getAutoclassEnabled());
    GoogleBucketResource retrievedBucket =
        resourceDao.getBucket(bucketName, projectResource.getId());
    assertTrue("Correct autoclass setting is returned", retrievedBucket.getAutoclassEnabled());
  }

  @Test
  public void testAutoclassDisabledFlag() {
    boolean autoclassEnabled = false;
    UUID bucketId = createBucketDbEntry(projectResource, autoclassEnabled);
    GoogleBucketResource bucket = resourceDao.retrieveBucketById(bucketId);
    assertFalse("Correct autoclass setting is returned", bucket.getAutoclassEnabled());
    GoogleBucketResource retrievedBucket =
        resourceDao.getBucket(bucketName, projectResource.getId());
    assertFalse("Correct autoclass setting is returned", retrievedBucket.getAutoclassEnabled());
  }

  @Test(expected = Exception.class)
  public void testRetrieveBucketByIdException() {
    UUID bucketId = UUID.randomUUID();
    // this should fail -> no bucket with this id
    resourceDao.retrieveBucketById(bucketId);
  }

  @Test(expected = Exception.class)
  public void testRetrieveBucketByNameException() {
    bucketName = "bucketDoesNotExist";
    // this should fail -> no bucket with this name
    resourceDao.retrieveBucketByName(bucketName);
  }

  @Test
  public void testGetAndUpdateBucketAutoclassByName() {
    bucketName = "bucket";
    GoogleProjectResource resource = dataset.getProjectResource();
    String flightId = UUID.randomUUID().toString();
    GoogleRegion region =
        (GoogleRegion)
            dataset.getDatasetSummary().getStorageResourceRegion(GoogleCloudResource.BUCKET);

    GoogleBucketResource bucketResource0 =
        resourceDao.createAndLockBucket(bucketName, resource, region, flightId, false);
    datasetBucketDao.createDatasetBucketLink(dataset.getId(), bucketResource0.getResourceId());

    GoogleBucketResource bucketResource1 = resourceDao.retrieveBucketByName(bucketName);
    assertFalse("Autoclass should be disabled", bucketResource1.getAutoclassEnabled());
    assertEquals(
        "Autoclass setting should be the same",
        bucketResource0.getAutoclassEnabled(),
        bucketResource1.getAutoclassEnabled());

    int rows = resourceDao.updateBucketAutoclassByName(bucketName, true);
    assertEquals("One row should be updated", 1, rows);

    GoogleBucketResource bucketResource2 = resourceDao.retrieveBucketByName(bucketName);
    assertTrue("Autoclass should be enabled", bucketResource2.getAutoclassEnabled());
  }

  private UUID createBucketDbEntry(GoogleProjectResource projectResource2) {
    return createBucketDbEntry(projectResource2, true);
  }

  private UUID createBucketDbEntry(
      GoogleProjectResource projectResource2, boolean autoclassEnabled) {
    ingestFileFlightId = UUID.randomUUID().toString();
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

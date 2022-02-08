package bio.terra.service.dataset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import bio.terra.app.model.GoogleCloudResource;
import bio.terra.app.model.GoogleRegion;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
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

  @Autowired private JsonLoader jsonLoader;

  @Autowired private DatasetDao datasetDao;

  @Autowired private DatasetBucketDao datasetBucketDao;

  @Autowired private ProfileDao profileDao;

  @Autowired private GoogleResourceDao resourceDao;

  private BillingProfileModel billingProfile;
  private GoogleProjectResource projectResource;
  private Dataset dataset;
  private UUID datasetId;

  private final List<UUID> billingProfileIds = new ArrayList<>();
  private final List<UUID> datasetIds = new ArrayList<>();
  private final List<UUID> bucketResourceIds = new ArrayList<>();
  private final List<UUID> projectIds = new ArrayList<>();

  @Before
  public void setup() throws IOException {
    BillingProfileRequestModel profileRequest = ProfileFixtures.randomBillingProfileRequest();
    billingProfile = profileDao.createBillingProfile(profileRequest, "testUser");
    billingProfileIds.add(billingProfile.getId());

    projectResource = ResourceFixtures.randomProjectResource(billingProfile);
    UUID projectId = resourceDao.createProject(projectResource);
    projectResource.id(projectId);
    projectIds.add(projectId);

    dataset = createMinimalDataset();
    datasetId = dataset.getId();
  }

  @After
  public void teardown() {

    datasetIds.forEach(
        datasetId -> {
          bucketResourceIds.forEach(
              bucketResourceId -> {
                try {
                  datasetBucketDao.deleteDatasetBucketLink(datasetId, bucketResourceId);
                } catch (Exception ex) {
                  logger.error(
                      "[CLEANUP] Unable to delete dataset bucket link for dataset {} and bucket resource {}",
                      datasetId,
                      bucketResourceId);
                }
              });
          try {
            datasetDao.delete(datasetId);
          } catch (Exception ex) {
            logger.error("[CLEANUP] Unable to delete dataset {}", datasetId);
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
  public void TestGetProjectForDatasetProfileCombo() throws Exception {
    UUID bucketResourceId = createBucketDbEntry(projectResource);
    datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);

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

    List<UUID> projectResourceIds_after =
        datasetBucketDao.getProjectResourceIdsForBucketPerDataset(datasetId);
    assertEquals(
        "Should return both projects for both billing profiles for dataset 1",
        2,
        projectResourceIds_after.size());

    // Get project given a new dataset
    Dataset dataset_second = createMinimalDataset();
    createBucketDbEntry(projectResource);
    assertNull(
        "Should NOT retrieve existing project",
        datasetBucketDao.getProjectResourceForBucket(
            dataset_second.getId(), billingProfile.getId()));
  }

  @Test
  public void TestDatasetBucketLink() {
    UUID bucketResourceId = createBucketDbEntry(projectResource);

    // initial check - link should not yet exist
    boolean linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertFalse("Link should not yet exist.", linkExists);

    // create link
    datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
    linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertTrue("Link should now exist.", linkExists);

    // delete link
    datasetBucketDao.deleteDatasetBucketLink(datasetId, bucketResourceId);
    linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertFalse("Link should no longer exists.", linkExists);
  }

  @Test
  public void TestMultipleLinks() {
    UUID bucketResourceId = createBucketDbEntry(projectResource);

    // initial check - link should not yet exist
    boolean linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertFalse("Link should not yet exist.", linkExists);

    // create link
    datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
    linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertTrue("Link should now exist.", linkExists);
    int linkCount =
        datasetBucketDao.datasetBucketSuccessfulIngestCount(datasetId, bucketResourceId);
    assertEquals("Link count should be 1.", 1, linkCount);

    // create link
    datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
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
  public void TestDecrementLink() {
    UUID bucketResourceId = createBucketDbEntry(projectResource);
    bucketResourceIds.add(bucketResourceId);

    // initial check - link should not yet exist
    boolean linkExists = datasetBucketDao.datasetBucketLinkExists(datasetId, bucketResourceId);
    assertFalse("Link should not yet exist.", linkExists);

    // create link
    datasetBucketDao.createDatasetBucketLink(datasetId, bucketResourceId);
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
  public void DatasetMustExistToLink() {
    // create bucket for dataset
    UUID bucketResourceId = createBucketDbEntry(projectResource);
    bucketResourceIds.add(bucketResourceId);

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
  public void BucketMustExistToLink() {
    // fake datasetId
    UUID randomBucketResourceId = UUID.randomUUID();

    // initial check - link should not yet exist
    boolean linkExists =
        datasetBucketDao.datasetBucketLinkExists(datasetId, randomBucketResourceId);
    assertFalse("Link should not yet exist.", linkExists);

    // this should fail -> no requires real dataset and bucket to link
    datasetBucketDao.createDatasetBucketLink(datasetId, randomBucketResourceId);
  }

  private Dataset createMinimalDataset() throws IOException {
    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject("dataset-minimal.json", DatasetRequestModel.class);
    String newName = datasetRequest.getName() + UUID.randomUUID();
    datasetRequest
        .name(newName)
        .defaultProfileId(billingProfile.getId())
        .cloudPlatform(CloudPlatform.GCP);
    Dataset dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest);
    dataset.projectResourceId(projectResource.getId());
    String createFlightId = UUID.randomUUID().toString();
    UUID datasetId = UUID.randomUUID();
    datasetIds.add(datasetId);
    dataset.id(datasetId);
    datasetDao.createAndLock(dataset, createFlightId);
    datasetDao.unlockExclusive(dataset.getId(), createFlightId);

    return dataset;
  }

  private UUID createBucketDbEntry(GoogleProjectResource projectResource2) {
    String ingestFileFlightId = UUID.randomUUID().toString();
    UUID googleBucketResourceId =
        resourceDao
            .createAndLockBucket(
                String.format("testbucket%s", ingestFileFlightId),
                projectResource2,
                (GoogleRegion)
                    dataset
                        .getDatasetSummary()
                        .getStorageResourceRegion(GoogleCloudResource.BUCKET),
                ingestFileFlightId)
            .getResourceId();
    bucketResourceIds.add(googleBucketResourceId);
    return googleBucketResourceId;
  }
}

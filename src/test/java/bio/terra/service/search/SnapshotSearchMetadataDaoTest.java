package bio.terra.service.search;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetUtils;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import bio.terra.service.snapshot.Snapshot;
import java.util.UUID;
import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@Category(Unit.class)
class SnapshotSearchMetadataDaoTest {

  @Autowired
  private GoogleResourceDao resourceDao;

  @Autowired
  private ProfileDao profileDao;

  private UUID profileId;
  private UUID projectId;
  private Dataset dataset;
  private Snapshot snapshot;

  @Before
  public void before() throws Exception {
    BillingProfileModel billingProfile =
        profileDao.createBillingProfile(ProfileFixtures.randomBillingProfileRequest(), "hi@hi.hi");
    profileId = billingProfile.getId();

    GoogleProjectResource projectResource = ResourceFixtures.randomProjectResource(billingProfile);
    var projectId = resourceDao.createProject(projectResource);

    DatasetRequestModel datasetRequest =
        jsonLoader.loadObject("snapshot-test-dataset.json", DatasetRequestModel.class);
    datasetRequest
        .name(datasetRequest.getName() + UUID.randomUUID())
        .defaultProfileId(profileId)
        .cloudPlatform(CloudPlatform.GCP);

    dataset = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest);
    dataset.projectResourceId(projectId);

    String createFlightId = UUID.randomUUID().toString();
    datasetId = UUID.randomUUID();
    dataset.id(datasetId);
    datasetDao.createAndLock(dataset, createFlightId);
    datasetDao.unlockExclusive(dataset.getId(), createFlightId);
    dataset = datasetDao.retrieve(datasetId);

  }

  @After
  public void after() throws Exception {

  }

  public void testPutGetDelete() {

  }
}

package bio.terra.service.resourcemanagement;

import bio.terra.common.Column;
import bio.terra.common.MetadataEnumeration;
import bio.terra.common.Table;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.common.fixtures.ResourceFixtures;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetBucketDao;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetUtils;
import bio.terra.service.dataset.exception.DatasetLockException;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.OneProjectPerProfileIdSelector;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.*;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class DatasetBucketLinkTest {

    private static final Logger logger = LoggerFactory.getLogger(DatasetBucketLinkTest.class);

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DatasetDao datasetDao;

    @Autowired
    private DatasetBucketDao datasetBucketDao;

    @Autowired
    private ProfileDao profileDao;

    @Autowired
    private GoogleResourceDao resourceDao;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    @Autowired
    private ResourceService resourceService;

    @Autowired
    private GoogleProjectService googleProjectService;

    @Autowired
    private GoogleBucketService googleBucketService;

    @Autowired
    private OneProjectPerProfileIdSelector oneProjectPerProfileIdSelector;

    private BillingProfileModel billingProfile1;
    private GoogleBucketResource bucket1;
    private UUID projectId1;
    private Dataset dataset1;

    private BillingProfileModel billingProfile2;
    private GoogleBucketResource bucket2;
    private UUID projectId2;
    private Dataset dataset2;


    @Before
    public void setup() throws IOException, InterruptedException {
        logger.info("-------------------Setup----------------------");
        // Two billing profiles
        BillingProfileRequestModel profileRequest1 = ProfileFixtures.randomBillingProfileRequest();
        billingProfile1 = profileDao.createBillingProfile(profileRequest1, "hi@hi.hi");

        BillingProfileRequestModel profileRequest2 = ProfileFixtures.randomBillingProfileRequest();
        billingProfile2 = profileDao.createBillingProfile(profileRequest2, "hi@hi.hi");

        // two google project resources
        GoogleProjectResource projectResource1 = ResourceFixtures.randomProjectResourceAndName(billingProfile1);
        projectId1 = resourceDao.createProject(projectResource1);

        GoogleProjectResource projectResource2 = ResourceFixtures.randomProjectResourceAndName(billingProfile2);
        projectId2 = resourceDao.createProject(projectResource2);

        // Create dataset 1
        DatasetRequestModel datasetRequest1 = jsonLoader.loadObject("dataset-minimal.json", DatasetRequestModel.class);
        datasetRequest1.name(datasetRequest1.getName() + UUID.randomUUID().toString())
            .defaultProfileId(billingProfile1.getId());
        dataset1 = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest1);
        dataset1.projectResourceId(projectId1);
        String createFlightId = UUID.randomUUID().toString();
        dataset1.id(UUID.randomUUID());
        datasetDao.createAndLock(dataset1, createFlightId);
        datasetDao.unlockExclusive(dataset1.getId(), createFlightId);

        // Create dataset 2
        DatasetRequestModel datasetRequest2 = jsonLoader.loadObject("dataset-minimal.json", DatasetRequestModel.class);
        datasetRequest2.name(datasetRequest2.getName() + UUID.randomUUID().toString())
            .defaultProfileId(billingProfile2.getId());
        dataset2 = DatasetUtils.convertRequestWithGeneratedNames(datasetRequest2);
        dataset2.projectResourceId(projectId2);
        String createFlightId2 = UUID.randomUUID().toString();
        dataset2.id(UUID.randomUUID());
        datasetDao.createAndLock(dataset2, createFlightId2);
        datasetDao.unlockExclusive(dataset2.getId(), createFlightId2);

        // create bucket 1
        String flightId = UUID.randomUUID().toString();
        // Every bucket needs to live in a project, so we get or create a project first
        // Q: Should the bucket projects be different from project that we created dataset with?
        /*GoogleProjectResource bucketProjectResource1 = googleProjectService.getOrCreateProject(
            oneProjectPerProfileIdSelector.projectIdForFile(dataset1.getName(), billingProfile1),
            billingProfile1,
            null);*/

        bucket1 = googleBucketService.getOrCreateBucket(
            oneProjectPerProfileIdSelector.bucketForFile(dataset1.getName(), billingProfile1),
            projectResource1,
            flightId);

        // create bucket 2
        String flightId2 = UUID.randomUUID().toString();
        /*GoogleProjectResource bucketProjectResource2 = googleProjectService.getOrCreateProject(
            oneProjectPerProfileIdSelector.projectIdForFile(dataset2.getName(), billingProfile2),
            billingProfile2,
            null);*/

        bucket2 = googleBucketService.getOrCreateBucket(
            oneProjectPerProfileIdSelector.bucketForFile(dataset2.getName(), billingProfile2),
            projectResource2,
            flightId2);
        logger.info("-------------------Test----------------------");
    }

    @After
    public void teardown() {
        logger.info("-------------------Cleanup----------------------");
        datasetBucketDao.deleteDatasetBucketLink(dataset1.getId(), bucket1.getResourceId());
        datasetBucketDao.deleteDatasetBucketLink(dataset2.getId(), bucket2.getResourceId());
        datasetDao.delete(dataset1.getId());
        datasetDao.delete(dataset2.getId());
        resourceDao.deleteProject(projectId1);
        resourceDao.deleteProject(projectId2);
        profileDao.deleteBillingProfileById(UUID.fromString(billingProfile1.getId()));
        profileDao.deleteBillingProfileById(UUID.fromString(billingProfile2.getId()));
    }

    @Test
    public void TestDatasetBucketBillingProfileMapping() throws Exception {

        assertNotEquals("Buckets should be named differently", bucket1.getName(), bucket2.getName());

        //initial check - link should not yet exist
        boolean linkExists = datasetBucketDao.datasetBucketLinkExists(dataset1.getId(), bucket1.getResourceId());
        assertFalse("Link should not yet exist.", linkExists);

        linkExists = datasetBucketDao.datasetBucketLinkExists(dataset2.getId(), bucket2.getResourceId());
        assertFalse("Link should not yet exist.", linkExists);

        // create link
        datasetBucketDao.createDatasetBucketLink(dataset1.getId(), bucket1.getResourceId());
        linkExists = datasetBucketDao.datasetBucketLinkExists(dataset1.getId(), bucket1.getResourceId());
        assertTrue("Link should now exist.", linkExists);

        // create link
        datasetBucketDao.createDatasetBucketLink(dataset2.getId(), bucket2.getResourceId());
        linkExists = datasetBucketDao.datasetBucketLinkExists(dataset2.getId(), bucket2.getResourceId());
        assertTrue("Link should now exist.", linkExists);


    }
}

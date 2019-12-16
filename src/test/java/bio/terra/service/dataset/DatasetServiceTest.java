package bio.terra.service.dataset;

import bio.terra.common.MetadataEnumeration;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.JsonLoader;
import bio.terra.common.fixtures.ProfileFixtures;
import bio.terra.model.AssetModel;
import bio.terra.model.DatasetRequestModel;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.resourcemanagement.BillingProfile;
import bio.terra.service.resourcemanagement.ProfileDao;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.junit4.SpringRunner;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class DatasetServiceTest {

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DatasetDao datasetDao;

    @Autowired
    private DatasetService datasetService;

    @Autowired
    private ProfileDao profileDao;

    @Autowired
    private NamedParameterJdbcTemplate jdbcTemplate;

    private BillingProfile billingProfile;

    private UUID createDataset(DatasetRequestModel datasetRequest, String newName) {
        datasetRequest.name(newName).defaultProfileId(billingProfile.getId().toString());
        return datasetDao.create(DatasetJsonConversion.datasetRequestToDataset(datasetRequest));
    }

    private UUID createDataset(String datasetFile) throws IOException {
        DatasetRequestModel datasetRequest = jsonLoader.loadObject(datasetFile, DatasetRequestModel.class);
        return createDataset(datasetRequest, datasetRequest.getName() + UUID.randomUUID().toString());
    }

    @Before
    public void setup() {
        billingProfile = ProfileFixtures.randomBillingProfile();
        UUID profileId = profileDao.createBillingProfile(billingProfile);
        billingProfile.id(profileId);
    }

    @After
    public void teardown() {
        profileDao.deleteBillingProfileById(billingProfile.getId());
    }

    @Test(expected = DatasetNotFoundException.class)
    public void datasetDeleteTest() throws IOException {
        UUID datasetId = createDataset("dataset-minimal.json");
        assertThat("dataset delete signals success", datasetDao.delete(datasetId), equalTo(true));
        datasetDao.retrieve(datasetId);
    }

    @Test
    public void addDatasetAssetSpecifications() throws Exception {
        UUID datasetId = createDataset("dataset-minimal.json");
        List<UUID> datasetIds = new ArrayList<>();
        datasetIds.add(datasetId);

        String assetName = "asset name";

        AssetModel assetModel = new AssetModel().name(assetName);

        // add asset to dataset
        datasetService.addDatasetAssetSpecifications(datasetId.toString(), assetModel, null);

        // get dataset
        Dataset dataset = datasetDao.retrieve(datasetId);

        // make sure the dataset has the expected asset
        assertThat("dataset has one asset spec", dataset.getAssetSpecifications().size(), equalTo(1));
        assertThat("dataset has expected asset", dataset.getAssetSpecificationByName(assetName).isPresent(),
            equalTo(true));

        datasetDao.delete(datasetId);
    }

    @Test
    public void removeDatasetAssetSpecifications() throws Exception {
        UUID datasetId = createDataset("dataset-minimal.json");
        List<UUID> datasetIds = new ArrayList<>();
        datasetIds.add(datasetId);

        String assetName = "asset name";

        AssetModel assetModel = new AssetModel().name(assetName);

        // add asset to dataset
        datasetService.addDatasetAssetSpecifications(datasetId.toString(), assetModel, null);

        // get dataset
        Dataset datasetWAsset = datasetDao.retrieve(datasetId);

        // make sure the dataset has the expected asset
        assertThat("dataset has one asset spec", datasetWAsset.getAssetSpecifications().size(), equalTo(1));
        assertThat("dataset has expected asset", datasetWAsset.getAssetSpecificationByName(assetName).isPresent(),
            equalTo(true));


        // remove asset from dataset
        datasetService.removeDatasetAssetSpecifications(datasetId.toString(), assetModel.getName(),null);

        // get dataset
        Dataset dataset = datasetDao.retrieve(datasetId);

        // make sure the dataset has the expected asset
        assertThat("dataset has one asset spec", dataset.getAssetSpecifications().size(), equalTo(0));

        datasetDao.delete(datasetId);
    }
}

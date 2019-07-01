package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.fixtures.JsonLoader;
import bio.terra.integration.auth.AuthService;
import bio.terra.integration.auth.Users;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.DeleteResponseModel;
import bio.terra.model.EnumerateDrDatasetModel;
import bio.terra.model.DrDatasetModel;
import bio.terra.model.DrDatasetSummaryModel;
import bio.terra.service.SamClientService;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;


@RunWith(SpringRunner.class)
@SpringBootTest
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class DrDatasetTest {
    private static final String omopDatasetName = "it_dataset_omop";
    private static final String omopDatasetDesc =
        "OMOP schema based on BigQuery schema from https://github.com/OHDSI/CommonDataModel/wiki";
    private static Logger logger = LoggerFactory.getLogger(DrDatasetTest.class);

    @Autowired
    private DataRepoClient dataRepoClient;

    @Autowired
    private JsonLoader jsonLoader;

    @Autowired
    private DataRepoFixtures dataRepoFixtures;

    @Autowired
    private Users users;

    @Autowired
    private AuthService authService;


    private TestConfiguration.User steward;
    private TestConfiguration.User custodian;
    private TestConfiguration.User reader;
    private String stewardToken;
    private String custodianToken;
    private String readerToken;


    @Before
    public void setup() throws Exception {
        steward = users.getUserForRole("steward");
        custodian = users.getUserForRole("custodian");
        reader = users.getUserForRole("reader");
        stewardToken = authService.getAuthToken(steward.getEmail());
        custodianToken = authService.getAuthToken(custodian.getEmail());
        readerToken = authService.getAuthToken(reader.getEmail());
        logger.info("steward: " + steward.getName() + "; custodian: " + custodian.getName() +
            "; reader: " + reader.getName());
    }

    @Test
    public void datasetHappyPath() throws Exception {
        DrDatasetSummaryModel summaryModel = dataRepoFixtures.createDataset(stewardToken,
            "it-dataset-omop.json");
        try {
            logger.info("dataset id is " + summaryModel.getId());
            assertThat(summaryModel.getName(), startsWith(omopDatasetName));
            assertThat(summaryModel.getDescription(), equalTo(omopDatasetDesc));

            DrDatasetModel datasetModel = dataRepoFixtures.getDataset(stewardToken, summaryModel.getId());

            assertThat(datasetModel.getName(), startsWith(omopDatasetName));
            assertThat(datasetModel.getDescription(), equalTo(omopDatasetDesc));

            EnumerateDrDatasetModel enumerateDatasetModel = dataRepoFixtures.enumerateStudies(stewardToken);
            boolean found = false;
            for (DrDatasetSummaryModel oneDataset : enumerateDatasetModel.getItems()) {
                if (oneDataset.getId().equals(datasetModel.getId())) {
                    assertThat(oneDataset.getName(), startsWith(omopDatasetName));
                    assertThat(oneDataset.getDescription(), equalTo(omopDatasetDesc));
                    found = true;
                    break;
                }
            }
            assertTrue("dataset was found in enumeration", found);

            // test allowable permissions

            dataRepoFixtures.addDatasetPolicyMember(
                stewardToken,
                summaryModel.getId(),
                SamClientService.DataRepoRole.CUSTODIAN,
                custodian.getEmail());
            DataRepoResponse<EnumerateDrDatasetModel> enumStudies =
                dataRepoFixtures.enumerateStudiesRaw(custodianToken);
            assertThat("Custodian is authorized to enumerate datasets",
                enumStudies.getStatusCode(),
                equalTo(HttpStatus.OK));

        } finally {
            logger.info("deleting dataset");
            dataRepoFixtures.deleteDatasetRaw(stewardToken, summaryModel.getId());
        }
    }

    @Test
    public void datasetUnauthorizedPermissionsTest() throws Exception {

        DataRepoResponse<DrDatasetSummaryModel> datasetSumRespCust = dataRepoFixtures.createDatasetRaw(
            custodianToken, "dataset-minimal.json");
        assertThat("Custodian is not authorized to create a dataset",
            datasetSumRespCust.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        DataRepoResponse<DrDatasetSummaryModel> datasetSumRespReader = dataRepoFixtures.createDatasetRaw(
            readerToken, "dataset-minimal.json");
        assertThat("Reader is not authorized to create a dataset",
            datasetSumRespReader.getStatusCode(),
            equalTo(HttpStatus.UNAUTHORIZED));

        EnumerateDrDatasetModel enumStudiesResp = dataRepoFixtures.enumerateStudies(readerToken);
        assertThat("Reader does not have access to datasets",
            enumStudiesResp.getTotal(),
            equalTo(0));

        DrDatasetSummaryModel summaryModel = null;
        try {
            summaryModel = dataRepoFixtures.createDataset(stewardToken, "dataset-minimal.json");
            logger.info("dataset id is " + summaryModel.getId());

            DataRepoResponse<DrDatasetModel> getDatasetResp = dataRepoFixtures.getDatasetRaw(readerToken,
                summaryModel.getId());
            assertThat("Reader is not authorized to get dataset",
                getDatasetResp.getStatusCode(),
                equalTo(HttpStatus.UNAUTHORIZED));

            DataRepoResponse<DeleteResponseModel> deleteResp1 = dataRepoFixtures.deleteDatasetRaw(
                readerToken, summaryModel.getId());
            assertThat("Reader is not authorized to delete datasets",
                deleteResp1.getStatusCode(),
                equalTo(HttpStatus.UNAUTHORIZED));

            DataRepoResponse<DeleteResponseModel> deleteResp2 = dataRepoFixtures.deleteDatasetRaw(
                custodianToken, summaryModel.getId());
            assertThat("Custodian is not authorized to delete datasets",
                deleteResp2.getStatusCode(),
                equalTo(HttpStatus.UNAUTHORIZED));
        } finally {
            if (summaryModel != null)
                dataRepoFixtures.deleteDataset(stewardToken, summaryModel.getId());
        }
    }

}

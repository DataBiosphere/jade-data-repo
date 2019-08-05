package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.integration.auth.AuthService;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.DRSObject;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetSummaryModel;
import com.google.cloud.bigquery.BigQuery;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest // TODO: Do we need this?
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class DrsTest extends UsersBase {
    private static final Logger logger = LoggerFactory.getLogger(DrsTest.class);

    @Autowired private DataRepoFixtures dataRepoFixtures;
    @Autowired private EncodeFixture encodeFixture;
    @Autowired private AuthService authService;
    @Autowired private TestConfiguration testConfiguration;

    private String readerToken;
    private DatasetModel datasetModel;

    @Before
    public void setup() throws Exception {
        super.setup();
        readerToken = authService.getDirectAccessAuthToken(reader().getEmail());
        DatasetSummaryModel datasetSummary = encodeFixture.setupEncode(steward(), custodian(), reader());
        datasetModel = dataRepoFixtures.getDataset(custodian(), datasetSummary.getId());
    }

    @Test
    public void drsHackyTest() throws Exception {
        // Get a DRS ID from the dataset
        BigQuery bigQueryReader = BigQueryFixtures.getBigQuery(testConfiguration.getGoogleProjectId(), readerToken);
        String drsObjectId = BigQueryFixtures.queryForDrsId(bigQueryReader,
            datasetModel,
            "file",
            "file_ref");

        // DRS lookup the file
        DRSObject drsObject = dataRepoFixtures.drsGetObject(reader(), drsObjectId);

        // <<< YOU ARE HERE >>>
        // use the file path parent to get a dir path
        // lookup the dir path
        // generate a DRS ID
        // DRS lookup the dire


    }




}

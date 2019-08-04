package bio.terra.integration;

import bio.terra.category.Integration;
import bio.terra.integration.auth.AuthService;
import bio.terra.integration.configuration.TestConfiguration;
import bio.terra.model.DatasetSummaryModel;
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
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class DrsTest extends UsersBase {
    private static final Logger logger = LoggerFactory.getLogger(DrsTest.class);

    @Autowired private DataRepoFixtures dataRepoFixtures;
    @Autowired private EncodeFixture encodeFixture;
    @Autowired private AuthService authService;
    @Autowired private TestConfiguration testConfiguration;

    private String readerToken;
    private DatasetSummaryModel datasetSummary;

    @Before
    public void setup() throws Exception {
        super.setup();
        readerToken = authService.getDirectAccessAuthToken(reader().getEmail());
        datasetSummary = encodeFixture.setupEncode(steward(), custodian(), reader());
    }

    @Test
    public void drsTest() {


        BigQueryProject bigQueryProjectReader = getBigQueryProject(testConfiguration.getGoogleProjectId(), readerToken);
        String query = String.format("SELECT file_ref FROM `%s.%s.file`",
            bigQueryProject.getProjectId(), datasetSummaryModel.getName());


        TableResult ids = bigQueryProjectReader.query(query);

        String drsId = null;
        for (FieldValueList fieldValueList : ids.iterateAll()) {
            drsId = fieldValueList.get(0).getStringValue();
        }



        // query for one bam file
        // DRS lookup
        // use the path - 1 to get a dir path
        // lookup the dir path
        // generate a DRS ID
        // DRS lookup







    }



}

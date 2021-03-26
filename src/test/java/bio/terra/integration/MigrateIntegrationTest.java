package bio.terra.integration;

import bio.terra.common.auth.AuthService;
import bio.terra.common.category.Integration;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.UsersBase;
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
@ActiveProfiles({"google", "integrationtest"})
@AutoConfigureMockMvc
@Category(Integration.class)
public class MigrateIntegrationTest extends UsersBase {
    private static Logger logger = LoggerFactory.getLogger(bio.terra.service.dataset.DatasetIntegrationTest.class);

    @Autowired private DataRepoFixtures dataRepoFixtures;

    private String profileId;

    @Before
    public void setup() throws Exception {
        super.setup();
        dataRepoFixtures.resetConfig(admin());
    }

    @After
    public void teardown() throws Exception {
        dataRepoFixtures.resetConfig(steward());
        if (profileId != null) {
            dataRepoFixtures.deleteProfileLog(steward(), profileId);
        }
    }

    // Test that when "false" the database does not migrate
    @Test
    public void updateNotMigrateFlight() throws Exception {
        // create something in database so you can check if it still exists after the migrate
        profileId = dataRepoFixtures.createBillingProfile(steward()).getId();

        // TODO: This fails with internal stairway error
        dataRepoFixtures.migrateDatabases(admin(), "false");

        // Should be able to successfully retrieve the profile
        dataRepoFixtures.retrieveBillingProfile(steward(), profileId);
    }

    // TODO: Add Test that flight fails when non-admin tries to run the flight

    // TODO: Add test and ignore test that runs the drop all on start
    //  & checks that things are missing in both datarepo and stairway

}

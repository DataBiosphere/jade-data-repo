package bio.terra.app.controller;

import bio.terra.common.category.Integration;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.UsersBase;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * This is meant to be a very lightweight integration test to make sure that SAM actions are used as expected.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class RepositoryApiControllerAccessTest extends UsersBase {

    @Autowired
    private DataRepoFixtures dataRepoFixtures;

    @Before
    public void setup() throws Exception {
        super.setup();
    }

    @Test
    public void testGetConfigList() throws Exception {
        // Assume this call is successful
        dataRepoFixtures.getConfigList(steward());

        // This call should be unsuccessful
        assertThat(dataRepoFixtures.getConfigListRaw(reader()).getStatusCode())
            .isEqualTo(HttpStatus.UNAUTHORIZED);
    }
}

package bio.terra.app.controller;

import bio.terra.common.category.Integration;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.UsersBase;
import bio.terra.model.ConfigParameterModel;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigModel;
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

import static bio.terra.service.configuration.ConfigEnum.SAM_RETRY_INITIAL_WAIT_SECONDS;
import static bio.terra.service.configuration.ConfigEnum.SAM_TIMEOUT_FAULT;
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

    @Test
    public void testSetConfigList() throws Exception {
        dataRepoFixtures.resetConfig(steward());
        ConfigGroupModel configGroup = new ConfigGroupModel()
            .label("testSetConfigList")
            .addGroupItem(new ConfigModel()
                .name(SAM_RETRY_INITIAL_WAIT_SECONDS.name())
                .configType(ConfigModel.ConfigTypeEnum.PARAMETER)
                .parameter(new ConfigParameterModel().value(String.valueOf(30))));

        // Assume this call is successful
        dataRepoFixtures.setConfigList(steward(), configGroup);

        // This call should be unsuccessful
        assertThat(dataRepoFixtures.setConfigListRaw(reader(), configGroup).getStatusCode())
            .isEqualTo(HttpStatus.UNAUTHORIZED);

        // Reset config changes
        dataRepoFixtures.resetConfig(steward());
    }

    @Test
    public void testGetConfig() throws Exception {
        assertThat(dataRepoFixtures.getConfig(steward(), SAM_RETRY_INITIAL_WAIT_SECONDS.name()).getStatusCode())
            .isEqualTo(HttpStatus.OK);

        assertThat(dataRepoFixtures.getConfig(reader(), SAM_RETRY_INITIAL_WAIT_SECONDS.name()).getStatusCode())
            .isEqualTo(HttpStatus.UNAUTHORIZED);
    }

    @Test
    public void testSetFault() throws Exception {
        assertThat(dataRepoFixtures.setFault(steward(), SAM_TIMEOUT_FAULT.name(), false).getStatusCode())
            .isEqualTo(HttpStatus.NO_CONTENT);

        assertThat(dataRepoFixtures.setFault(reader(), SAM_TIMEOUT_FAULT.name(), false).getStatusCode())
            .isEqualTo(HttpStatus.UNAUTHORIZED);

        // Reset config changes
        dataRepoFixtures.resetConfig(steward());
    }

    @Test
    public void testResetConfig() throws Exception {
        assertThat(dataRepoFixtures.resetConfig(steward()).getStatusCode())
            .isEqualTo(HttpStatus.NO_CONTENT);

        assertThat(dataRepoFixtures.resetConfig(reader()).getStatusCode())
            .isEqualTo(HttpStatus.UNAUTHORIZED);
    }
}

package bio.terra.app.controller;

import static bio.terra.service.configuration.ConfigEnum.SAM_RETRY_INITIAL_WAIT_SECONDS;
import static bio.terra.service.configuration.ConfigEnum.SAM_TIMEOUT_FAULT;
import static org.assertj.core.api.Assertions.assertThat;

import bio.terra.common.category.Integration;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.TestJobWatcher;
import bio.terra.integration.UsersBase;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigModel;
import bio.terra.model.ConfigParameterModel;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

/**
 * This is meant to be a very lightweight integration test to make sure that SAM actions are used as
 * expected.
 */
@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "integrationtest"})
@Category(Integration.class)
public class RepositoryApiControllerAccessTest extends UsersBase {

  @Autowired private DataRepoFixtures dataRepoFixtures;

  @Rule @Autowired public TestJobWatcher testWatcher;

  @Before
  public void setup() throws Exception {
    super.setup();
  }

  @Test
  public void testGetConfigList() throws Exception {
    // Assume this call is successful
    dataRepoFixtures.getConfigList(admin());

    // This call should be unsuccessful
    assertThat(dataRepoFixtures.getConfigListRaw(reader()).getStatusCode())
        .isEqualTo(HttpStatus.FORBIDDEN);
  }

  @Test
  public void testSetConfigList() throws Exception {
    dataRepoFixtures.resetConfig(admin());
    ConfigGroupModel configGroup =
        new ConfigGroupModel()
            .label("testSetConfigList")
            .addGroupItem(
                new ConfigModel()
                    .name(SAM_RETRY_INITIAL_WAIT_SECONDS.name())
                    .configType(ConfigModel.ConfigTypeEnum.PARAMETER)
                    .parameter(new ConfigParameterModel().value(String.valueOf(30))));

    // Assume this call is successful
    dataRepoFixtures.setConfigList(admin(), configGroup);

    // This call should be unsuccessful
    assertThat(dataRepoFixtures.setConfigListRaw(reader(), configGroup).getStatusCode())
        .isEqualTo(HttpStatus.FORBIDDEN);

    // Reset config changes
    dataRepoFixtures.resetConfig(admin());
  }

  @Test
  public void testGetConfig() throws Exception {
    assertThat(
            dataRepoFixtures
                .getConfig(admin(), SAM_RETRY_INITIAL_WAIT_SECONDS.name())
                .getStatusCode())
        .isEqualTo(HttpStatus.OK);

    assertThat(
            dataRepoFixtures
                .getConfig(reader(), SAM_RETRY_INITIAL_WAIT_SECONDS.name())
                .getStatusCode())
        .isEqualTo(HttpStatus.FORBIDDEN);
  }

  @Test
  public void testSetFault() throws Exception {
    assertThat(dataRepoFixtures.setFault(admin(), SAM_TIMEOUT_FAULT.name(), false).getStatusCode())
        .isEqualTo(HttpStatus.NO_CONTENT);

    assertThat(dataRepoFixtures.setFault(reader(), SAM_TIMEOUT_FAULT.name(), false).getStatusCode())
        .isEqualTo(HttpStatus.FORBIDDEN);

    // Reset config changes
    dataRepoFixtures.resetConfig(admin());
  }

  @Test
  public void testResetConfig() throws Exception {
    assertThat(dataRepoFixtures.resetConfig(admin()).getStatusCode())
        .isEqualTo(HttpStatus.NO_CONTENT);

    assertThat(dataRepoFixtures.resetConfig(reader()).getStatusCode())
        .isEqualTo(HttpStatus.FORBIDDEN);
  }
}

package bio.terra.app.controller;

import static bio.terra.service.configuration.ConfigEnum.SAM_RETRY_INITIAL_WAIT_SECONDS;
import static bio.terra.service.configuration.ConfigEnum.SAM_TIMEOUT_FAULT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Integration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.integration.DataRepoFixtures;
import bio.terra.integration.IntegrationTestConfiguration;
import bio.terra.integration.IntegrationTestConfiguration;
import bio.terra.integration.Users;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigModel;
import bio.terra.model.ConfigParameterModel;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

/**
 * This is meant to be a very lightweight integration test to make sure that SAM actions are used as
 * expected.
 */
@ExtendWith(SpringExtension.class)
@SpringBootTest(classes = IntegrationTestConfiguration.class)
@ActiveProfiles({"google", "integrationtest"})
@Tag(Integration.TAG)
class RepositoryApiControllerAccessTest {

  @Autowired private DataRepoFixtures dataRepoFixtures;
  @Autowired private Users users;

  private TestConfiguration.User admin;
  private TestConfiguration.User reader;

  @BeforeEach
  public void setup() throws Exception {
    admin = users.admin();
    reader = users.reader();
  }

  @Test
  void testGetConfigList() throws Exception {
    // Assume this call is successful
    dataRepoFixtures.getConfigList(admin);

    // This call should be unsuccessful
    assertThat(
        dataRepoFixtures.getConfigListRaw(reader).getStatusCode(), is(HttpStatus.FORBIDDEN));
  }

  @Test
  void testSetConfigList() throws Exception {
    dataRepoFixtures.resetConfig(admin);
    ConfigGroupModel configGroup =
        new ConfigGroupModel()
            .label("testSetConfigList")
            .addGroupItem(
                new ConfigModel()
                    .name(SAM_RETRY_INITIAL_WAIT_SECONDS.name())
                    .configType(ConfigModel.ConfigTypeEnum.PARAMETER)
                    .parameter(new ConfigParameterModel().value(String.valueOf(30))));

    // Assume this call is successful
    dataRepoFixtures.setConfigList(admin, configGroup);

    // This call should be unsuccessful
    assertThat(
        dataRepoFixtures.setConfigListRaw(reader, configGroup).getStatusCode(),
        is(HttpStatus.FORBIDDEN));

    // Reset config changes
    dataRepoFixtures.resetConfig(admin);
  }

  @Test
  void testGetConfig() throws Exception {
    assertThat(
        dataRepoFixtures.getConfig(admin, SAM_RETRY_INITIAL_WAIT_SECONDS.name()).getStatusCode(),
        is(HttpStatus.OK));

    assertThat(
        dataRepoFixtures.getConfig(reader, SAM_RETRY_INITIAL_WAIT_SECONDS.name()).getStatusCode(),
        is(HttpStatus.FORBIDDEN));
  }

  @Test
  void testSetFault() throws Exception {
    assertThat(
        dataRepoFixtures.setFault(admin, SAM_TIMEOUT_FAULT.name(), false).getStatusCode(),
        is(HttpStatus.NO_CONTENT));

    assertThat(
        dataRepoFixtures.setFault(reader, SAM_TIMEOUT_FAULT.name(), false).getStatusCode(),
        is(HttpStatus.FORBIDDEN));

    // Reset config changes
    dataRepoFixtures.resetConfig(admin);
  }

  @Test
  void testResetConfig() throws Exception {
    assertThat(dataRepoFixtures.resetConfig(admin).getStatusCode(), is(HttpStatus.NO_CONTENT));

    assertThat(dataRepoFixtures.resetConfig(reader).getStatusCode(), is(HttpStatus.FORBIDDEN));
  }
}

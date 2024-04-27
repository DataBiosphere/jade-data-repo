package bio.terra.service.auth.iam.sam;

import static bio.terra.service.configuration.ConfigEnum.SAM_OPERATION_TIMEOUT_SECONDS;
import static bio.terra.service.configuration.ConfigEnum.SAM_RETRY_INITIAL_WAIT_SECONDS;
import static bio.terra.service.configuration.ConfigEnum.SAM_RETRY_MAXIMUM_WAIT_SECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.SamConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigModel;
import bio.terra.model.ConfigParameterModel;
import bio.terra.service.auth.iam.exception.IamInternalServerErrorException;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import com.google.api.client.http.HttpStatusCodes;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
class SamRetryTest {
  private ConfigurationService configService;

  private int count;

  @BeforeEach
  void setup() {
    count = 0;
    configService =
        new ConfigurationService(
            mock(SamConfiguration.class),
            mock(GoogleResourceConfiguration.class),
            mock(ApplicationConfiguration.class));
  }

  @Test
  void testRetryTimeout() {
    setSamParams("testRetryTimeout", 1, 3, 10);
    assertThrows(
        IamInternalServerErrorException.class,
        () -> SamRetry.retry(configService, () -> testRetryFinishInner(100)));
  }

  @Test
  void testRetryFinish() throws Exception {
    setSamParams("testRetryFinish", 2, 5, 10);
    SamRetry.retry(configService, () -> testRetryFinishInner(2));
  }

  // Make this "Inner" to mimic the structure of the SamIam code
  // It "fails" twice and then succeeds
  private boolean testRetryFinishInner(int failCount) throws ApiException {
    if (count < failCount) {
      count++;
      throw new ApiException(HttpStatusCodes.STATUS_CODE_SERVER_ERROR, "testing");
    }
    return true;
  }

  @Test
  void testRetryVoidTimeout() throws Exception {
    setSamParams("testRetryTimeout", 1, 3, 10);
    assertThrows(
        IamInternalServerErrorException.class,
        () -> SamRetry.retry(configService, () -> testRetryVoidFinishInner(100)));
  }

  @Test
  void testRetryVoidFinish() throws Exception {
    setSamParams("testRetryFinish", 2, 5, 10);
    SamRetry.retry(configService, () -> testRetryVoidFinishInner(2));
  }

  // Make this "Inner" to mimic the structure of the SamIam code
  // It "fails" twice and then succeeds
  private void testRetryVoidFinishInner(int failCount) throws ApiException {
    if (count < failCount) {
      count++;
      throw new ApiException(HttpStatusCodes.STATUS_CODE_SERVER_ERROR, "testing");
    }
  }

  private void setSamParams(String label, int initialWait, int maxWait, int operationTimeout) {
    ConfigGroupModel groupModel =
        new ConfigGroupModel()
            .label(label)
            .addGroupItem(
                new ConfigModel()
                    .name(SAM_RETRY_INITIAL_WAIT_SECONDS.name())
                    .configType(ConfigModel.ConfigTypeEnum.PARAMETER)
                    .parameter(new ConfigParameterModel().value(String.valueOf(initialWait))))
            .addGroupItem(
                new ConfigModel()
                    .name(SAM_RETRY_MAXIMUM_WAIT_SECONDS.name())
                    .configType(ConfigModel.ConfigTypeEnum.PARAMETER)
                    .parameter(new ConfigParameterModel().value(String.valueOf(maxWait))))
            .addGroupItem(
                new ConfigModel()
                    .name(SAM_OPERATION_TIMEOUT_SECONDS.name())
                    .configType(ConfigModel.ConfigTypeEnum.PARAMETER)
                    .parameter(new ConfigParameterModel().value(String.valueOf(operationTimeout))));
    configService.setConfig(groupModel);
  }
}

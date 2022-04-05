package bio.terra.service.auth.iam.sam;

import static bio.terra.service.configuration.ConfigEnum.SAM_OPERATION_TIMEOUT_SECONDS;
import static bio.terra.service.configuration.ConfigEnum.SAM_RETRY_INITIAL_WAIT_SECONDS;
import static bio.terra.service.configuration.ConfigEnum.SAM_RETRY_MAXIMUM_WAIT_SECONDS;

import bio.terra.common.category.Unit;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigModel;
import bio.terra.model.ConfigParameterModel;
import bio.terra.service.auth.iam.exception.IamInternalServerErrorException;
import bio.terra.service.configuration.ConfigurationService;
import com.google.api.client.http.HttpStatusCodes;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class SamRetryTest {
  @Autowired private ConfigurationService configService;

  private int count;

  @Before
  public void setup() {
    count = 0;
  }

  @Test(expected = IamInternalServerErrorException.class)
  public void testRetryTimeout() throws Exception {
    setSamParams("testRetryTimeout", 1, 3, 10);
    SamRetry.retry(configService, () -> testRetryFinishInner(100));
  }

  @Test
  public void testRetryFinish() throws Exception {
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

  @Test(expected = IamInternalServerErrorException.class)
  public void testRetryVoidTimeout() throws Exception {
    setSamParams("testRetryTimeout", 1, 3, 10);
    SamRetry.retry(configService, () -> testRetryVoidFinishInner(100));
  }

  @Test
  public void testRetryVoidFinish() throws Exception {
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

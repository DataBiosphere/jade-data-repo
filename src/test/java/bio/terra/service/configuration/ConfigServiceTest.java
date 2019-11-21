package bio.terra.service.configuration;

import bio.terra.common.category.Unit;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigModel;
import bio.terra.model.ConfigParameterModel;
import bio.terra.service.configuration.exception.ConfigNotFoundException;
import bio.terra.service.configuration.exception.DuplicateConfigNameException;
import bio.terra.service.iam.sam.SamConfiguration;
import org.apache.commons.codec.binary.StringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static bio.terra.service.configuration.ConfigurationService.SAM_OPERATION_TIMEOUT_SECONDS;
import static bio.terra.service.configuration.ConfigurationService.SAM_RETRY_INITIAL_WAIT_SECONDS;
import static bio.terra.service.configuration.ConfigurationService.SAM_RETRY_MAXIMUM_WAIT_SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)

public class ConfigServiceTest {

    @Autowired
    private ConfigurationService configService;

    @Autowired
    private SamConfiguration samConfiguration;

    @Test
    public void configBasicTest() throws Exception {
        final int delta = 10;
        int retryInitialWaitSeconds = samConfiguration.getRetryInitialWaitSeconds();
        int retryMaximumWaitSeconds = samConfiguration.getRetryMaximumWaitSeconds();
        int operationTimeoutSeconds = samConfiguration.getOperationTimeoutSeconds();

        // Retrieve all config and make sure initialization worked
        List<ConfigModel> configModelList = configService.getConfigList();
        checkIntParamValue(configModelList, SAM_RETRY_INITIAL_WAIT_SECONDS, retryInitialWaitSeconds);
        checkIntParamValue(configModelList, SAM_RETRY_MAXIMUM_WAIT_SECONDS, retryMaximumWaitSeconds);
        checkIntParamValue(configModelList, SAM_OPERATION_TIMEOUT_SECONDS, operationTimeoutSeconds);

        // Set some config
        ConfigGroupModel groupModel = new ConfigGroupModel()
            .label("configBasicTest")
            .addGroupItem(new ConfigModel()
                .name(SAM_RETRY_INITIAL_WAIT_SECONDS)
                .configType(ConfigModel.ConfigTypeEnum.PARAMETER)
                .parameter(new ConfigParameterModel().value(String.valueOf(retryInitialWaitSeconds + delta))))
            .addGroupItem(new ConfigModel()
                .name(SAM_RETRY_MAXIMUM_WAIT_SECONDS)
                .configType(ConfigModel.ConfigTypeEnum.PARAMETER)
                .parameter(new ConfigParameterModel().value(String.valueOf(retryMaximumWaitSeconds + delta))));
        configService.setConfig(groupModel);

        // Retrieve specific config
        Integer expectedValue = retryInitialWaitSeconds + delta;
        ConfigModel configModel = configService.getConfig(SAM_RETRY_INITIAL_WAIT_SECONDS);
        assertThat(configModel.getConfigType(), equalTo(ConfigModel.ConfigTypeEnum.PARAMETER));
        assertThat("Int param matches", configModel.getParameter().getValue(),
            equalTo(expectedValue.toString()));

        // Reset config and check result
        configService.reset();

        configModelList = configService.getConfigList();
        checkIntParamValue(configModelList, SAM_RETRY_INITIAL_WAIT_SECONDS, retryInitialWaitSeconds);
        checkIntParamValue(configModelList, SAM_RETRY_MAXIMUM_WAIT_SECONDS, retryMaximumWaitSeconds);
        checkIntParamValue(configModelList, SAM_OPERATION_TIMEOUT_SECONDS, operationTimeoutSeconds);
    }

    @Test(expected = DuplicateConfigNameException.class)
    public void testDuplicateConfigException() throws Exception {
        configService.addParameter(SAM_RETRY_INITIAL_WAIT_SECONDS, 42);
    }

    @Test(expected = ConfigNotFoundException.class)
    public void testConfigNotFoundLookup() throws Exception {
        configService.getConfig("xyzzy");
    }

    @Test(expected = ConfigNotFoundException.class)
    public void testConfigNotFoundSet() throws Exception {
        ConfigGroupModel groupModel = new ConfigGroupModel()
            .label("configNotFoundSetTest")
            .addGroupItem(new ConfigModel()
                .name("xyzzy")
                .configType(ConfigModel.ConfigTypeEnum.PARAMETER)
                .parameter(new ConfigParameterModel().value(String.valueOf(22))));
        configService.setConfig(groupModel);
    }

    private void checkIntParamValue(List<ConfigModel> configModelList, String name, Integer expectedValue) {
        for (ConfigModel configModel : configModelList) {
            if (StringUtils.equals(configModel.getName(), name)) {
                assertThat(configModel.getConfigType(), equalTo(ConfigModel.ConfigTypeEnum.PARAMETER));
                assertThat("Int param matches", configModel.getParameter().getValue(),
                    equalTo(expectedValue.toString()));
                return;
            }
        }
        fail("Failed to find config param " + name);
    }

}

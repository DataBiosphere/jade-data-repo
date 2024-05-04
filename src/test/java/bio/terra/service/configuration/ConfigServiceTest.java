package bio.terra.service.configuration;

import static bio.terra.service.configuration.ConfigEnum.SAM_OPERATION_TIMEOUT_SECONDS;
import static bio.terra.service.configuration.ConfigEnum.SAM_RETRY_INITIAL_WAIT_SECONDS;
import static bio.terra.service.configuration.ConfigEnum.SAM_RETRY_MAXIMUM_WAIT_SECONDS;
import static bio.terra.service.configuration.ConfigEnum.UNIT_TEST_COUNTED_FAULT;
import static bio.terra.service.configuration.ConfigEnum.UNIT_TEST_SIMPLE_FAULT;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.SamConfiguration;
import bio.terra.app.controller.exception.ValidationException;
import bio.terra.common.category.Unit;
import bio.terra.model.ConfigFaultCountedModel;
import bio.terra.model.ConfigFaultModel;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigModel;
import bio.terra.model.ConfigParameterModel;
import bio.terra.service.configuration.exception.ConfigNotFoundException;
import bio.terra.service.configuration.exception.DuplicateConfigNameException;
import bio.terra.service.resourcemanagement.google.GoogleResourceConfiguration;
import java.util.List;
import org.apache.commons.codec.binary.StringUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class ConfigServiceTest {

  private SamConfiguration samConfiguration;

  private ConfigurationService configService;

  @BeforeEach
  void beforeEach() {
    samConfiguration = new SamConfiguration("", "", 5, 10, 15);
    configService =
        new ConfigurationService(
            samConfiguration,
            mock(GoogleResourceConfiguration.class),
            mock(ApplicationConfiguration.class));
  }

  @Test
  void configBasicTest() {
    // Order of tests can cause config state to change, so ensure config is reset otherwise tests
    // may fail
    configService.reset();

    final int delta = 10;
    int retryInitialWaitSeconds = samConfiguration.retryInitialWaitSeconds();
    int retryMaximumWaitSeconds = samConfiguration.retryMaximumWaitSeconds();
    int operationTimeoutSeconds = samConfiguration.operationTimeoutSeconds();

    // Retrieve all config and make sure initialization worked
    List<ConfigModel> configModelList = configService.getConfigList().getItems();
    checkIntParamValue(
        configModelList, SAM_RETRY_INITIAL_WAIT_SECONDS.name(), retryInitialWaitSeconds);
    checkIntParamValue(
        configModelList, SAM_RETRY_MAXIMUM_WAIT_SECONDS.name(), retryMaximumWaitSeconds);
    checkIntParamValue(
        configModelList, SAM_OPERATION_TIMEOUT_SECONDS.name(), operationTimeoutSeconds);

    // Set some config
    ConfigGroupModel groupModel =
        new ConfigGroupModel()
            .label("configBasicTest")
            .addGroupItem(
                new ConfigModel()
                    .name(SAM_RETRY_INITIAL_WAIT_SECONDS.name())
                    .configType(ConfigModel.ConfigTypeEnum.PARAMETER)
                    .parameter(
                        new ConfigParameterModel()
                            .value(String.valueOf(retryInitialWaitSeconds + delta))))
            .addGroupItem(
                new ConfigModel()
                    .name(SAM_RETRY_MAXIMUM_WAIT_SECONDS.name())
                    .configType(ConfigModel.ConfigTypeEnum.PARAMETER)
                    .parameter(
                        new ConfigParameterModel()
                            .value(String.valueOf(retryMaximumWaitSeconds + delta))));
    configService.setConfig(groupModel);

    // Retrieve specific config
    int expectedValue = retryInitialWaitSeconds + delta;
    ConfigModel configModel = configService.getConfig(SAM_RETRY_INITIAL_WAIT_SECONDS.name());
    assertThat(configModel.getConfigType(), equalTo(ConfigModel.ConfigTypeEnum.PARAMETER));
    assertThat(
        "Int param matches",
        configModel.getParameter().getValue(),
        equalTo(String.valueOf(expectedValue)));

    // Reset config and check result
    configService.reset();

    configModelList = configService.getConfigList().getItems();
    checkIntParamValue(
        configModelList, SAM_RETRY_INITIAL_WAIT_SECONDS.name(), retryInitialWaitSeconds);
    checkIntParamValue(
        configModelList, SAM_RETRY_MAXIMUM_WAIT_SECONDS.name(), retryMaximumWaitSeconds);
    checkIntParamValue(
        configModelList, SAM_OPERATION_TIMEOUT_SECONDS.name(), operationTimeoutSeconds);
  }

  @Test
  void testDuplicateConfigException() {
    assertThrows(
        DuplicateConfigNameException.class,
        () -> configService.addParameter(SAM_RETRY_INITIAL_WAIT_SECONDS, 42));
  }

  @Test
  void testConfigNotFoundLookup() {
    assertThrows(ConfigNotFoundException.class, () -> configService.getConfig("xyzzy"));
  }

  @Test
  void testConfigNotFoundSet() {
    ConfigGroupModel groupModel =
        new ConfigGroupModel()
            .label("configNotFoundSetTest")
            .addGroupItem(
                new ConfigModel()
                    .name("xyzzy")
                    .configType(ConfigModel.ConfigTypeEnum.PARAMETER)
                    .parameter(new ConfigParameterModel().value(String.valueOf(22))));
    assertThrows(ConfigNotFoundException.class, () -> configService.setConfig(groupModel));
  }

  private void checkIntParamValue(
      List<ConfigModel> configModelList, String name, int expectedValue) {
    for (ConfigModel configModel : configModelList) {
      if (StringUtils.equals(configModel.getName(), name)) {
        assertThat(configModel.getConfigType(), equalTo(ConfigModel.ConfigTypeEnum.PARAMETER));
        assertThat(
            "Int param matches",
            configModel.getParameter().getValue(),
            equalTo(String.valueOf(expectedValue)));
        return;
      }
    }
    fail("Failed to find config param " + name);
  }

  @Test
  void testFaultSimple() {
    configService.addFaultSimple(UNIT_TEST_SIMPLE_FAULT);

    boolean simpleTest = configService.testInsertFault(UNIT_TEST_SIMPLE_FAULT);
    assertFalse(simpleTest, "Simple fault is disabled");

    configService.setFault(UNIT_TEST_SIMPLE_FAULT.name(), true);
    simpleTest = configService.testInsertFault(UNIT_TEST_SIMPLE_FAULT);
    assertTrue(simpleTest, "Simple fault is enabled");
  }

  @Test
  void testCountedFixed() {
    setFaultCounted(5, 3, 20, ConfigFaultCountedModel.RateStyleEnum.FIXED);
    configService.setFault(UNIT_TEST_COUNTED_FAULT.name(), true);

    // These should be the skip 5
    tryCountedN(5, false);
    // first round of 20%: 4 off, 1 on
    tryCountedN(4, false);
    tryCountedN(1, true);
    // second round of 20%: 4 off, 1 on
    tryCountedN(4, false);
    tryCountedN(1, true);
    // third (last) round of 20%: 4 off, 1 on
    tryCountedN(4, false);
    tryCountedN(1, true);
    // now the fault should be done and not inserted again
    tryCountedN(10, false);

    // Update the fault definition to be always on for 10
    updateCountedFault(0, 10, 100, ConfigFaultCountedModel.RateStyleEnum.FIXED);
    tryCountedN(10, true);
    tryCountedN(10, false);

    // Update the fault definition to be always on for 1
    updateCountedFault(0, 1, 100, ConfigFaultCountedModel.RateStyleEnum.FIXED);
    tryCountedN(1, true);
    tryCountedN(10, false);

    // Check that the math works
    updateCountedFault(0, 10, 222, ConfigFaultCountedModel.RateStyleEnum.FIXED);
    tryCountedN(10, true);
    tryCountedN(10, false);

    // Check that insert forever works
    updateCountedFault(0, -1, 100, ConfigFaultCountedModel.RateStyleEnum.FIXED);
    tryCountedN(100, true);
  }

  @Test
  void testCountedRandom() {
    setFaultCounted(0, -1, 10, ConfigFaultCountedModel.RateStyleEnum.RANDOM);
    configService.setFault(UNIT_TEST_COUNTED_FAULT.name(), true);

    // It is problematic to get a consistent result from a probabilistic test.
    // Let's try this: 10,000 tests at 10% should do 1000 inserts. Test if it
    // is within 1%, so between 900 and 1100.
    int inserted = 0;
    for (int i = 0; i < 10000; i++) {
      if (configService.testInsertFault(UNIT_TEST_COUNTED_FAULT)) {
        inserted++;
      }
    }
    assertThat(inserted, allOf(greaterThan(900), lessThan(1100)));
  }

  @Test
  void testDuplicateFaultConfigException() {
    configService.addFaultCounted(
        UNIT_TEST_COUNTED_FAULT, 0, -1, 10, ConfigFaultCountedModel.RateStyleEnum.RANDOM);
    assertThrows(
        DuplicateConfigNameException.class,
        () ->
            configService.addFaultCounted(
                UNIT_TEST_COUNTED_FAULT, 0, -1, 10, ConfigFaultCountedModel.RateStyleEnum.RANDOM));
  }

  @Test
  void testMismatchedFaultTypeSet() {
    setFaultCounted(0, -1, 10, ConfigFaultCountedModel.RateStyleEnum.RANDOM);
    ConfigFaultModel faultModel =
        new ConfigFaultModel()
            .faultType(ConfigFaultModel.FaultTypeEnum.SIMPLE)
            .counted(null)
            .enabled(true);
    ConfigGroupModel groupModel =
        new ConfigGroupModel()
            .label("testMismatchedFaultTypeSet")
            .addGroupItem(
                new ConfigModel()
                    .name(UNIT_TEST_COUNTED_FAULT.name())
                    .configType(ConfigModel.ConfigTypeEnum.FAULT)
                    .fault(faultModel));
    assertThrows(ValidationException.class, () -> configService.setConfig(groupModel));
  }

  @Test
  void testMissingCountedModelSet() {
    setFaultCounted(0, -1, 10, ConfigFaultCountedModel.RateStyleEnum.RANDOM);
    ConfigFaultModel faultModel =
        new ConfigFaultModel()
            .faultType(ConfigFaultModel.FaultTypeEnum.COUNTED)
            .counted(null)
            .enabled(true);
    ConfigGroupModel groupModel =
        new ConfigGroupModel()
            .label("testMissingCountedModelSet")
            .addGroupItem(
                new ConfigModel()
                    .name(UNIT_TEST_COUNTED_FAULT.name())
                    .configType(ConfigModel.ConfigTypeEnum.FAULT)
                    .fault(faultModel));
    assertThrows(ValidationException.class, () -> configService.setConfig(groupModel));
  }

  private void tryCountedN(int iterations, boolean expected) {
    for (int i = 0; i < iterations; i++) {
      boolean test = configService.testInsertFault(UNIT_TEST_COUNTED_FAULT);
      assertThat("Correct fault insert result", test, equalTo(expected));
    }
  }

  private void updateCountedFault(
      int skipFor, int insert, int rate, ConfigFaultCountedModel.RateStyleEnum rateStyle) {
    ConfigFaultCountedModel countedModel =
        new ConfigFaultCountedModel()
            .skipFor(skipFor)
            .insert(insert)
            .rate(rate)
            .rateStyle(rateStyle);
    ConfigFaultModel faultModel =
        new ConfigFaultModel()
            .faultType(ConfigFaultModel.FaultTypeEnum.COUNTED)
            .counted(countedModel)
            .enabled(true);
    ConfigGroupModel groupModel =
        new ConfigGroupModel()
            .label("updateCountedFault")
            .addGroupItem(
                new ConfigModel()
                    .name(UNIT_TEST_COUNTED_FAULT.name())
                    .configType(ConfigModel.ConfigTypeEnum.FAULT)
                    .fault(faultModel));
    configService.setConfig(groupModel);
  }

  // Use the get interface to detect if the fault has been defined. If not, define it.
  // If so, update it.
  private void setFaultCounted(
      int skipFor, int insert, int rate, ConfigFaultCountedModel.RateStyleEnum rateStyle) {
    try {
      configService.getConfig("UNIT_TEST_COUNTED_FAULT");
      updateCountedFault(skipFor, insert, rate, rateStyle);
    } catch (ConfigNotFoundException ex) {
      configService.addFaultCounted(UNIT_TEST_COUNTED_FAULT, skipFor, insert, rate, rateStyle);
    }
  }
}

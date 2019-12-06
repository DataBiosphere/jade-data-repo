package bio.terra.service.configuration;

import bio.terra.model.ConfigFaultCountedModel;
import bio.terra.model.ConfigFaultModel;
import bio.terra.model.ConfigGroupModel;
import bio.terra.model.ConfigModel;
import bio.terra.service.configuration.exception.ConfigNotFoundException;
import bio.terra.service.configuration.exception.DuplicateConfigNameException;
import bio.terra.service.iam.sam.SamConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static bio.terra.service.configuration.ConfigEnum.SAM_OPERATION_TIMEOUT_SECONDS;
import static bio.terra.service.configuration.ConfigEnum.SAM_RETRY_INITIAL_WAIT_SECONDS;
import static bio.terra.service.configuration.ConfigEnum.SAM_RETRY_MAXIMUM_WAIT_SECONDS;

@Component
public class ConfigurationService {
    private final Logger logger = LoggerFactory.getLogger(ConfigurationService.class);

    private final SamConfiguration samConfiguration;

    private Map<ConfigEnum, ConfigBase> configuration = new HashMap<>();

    @Autowired
    public ConfigurationService(SamConfiguration samConfiguration) {
        this.samConfiguration = samConfiguration;
        setConfiguration();
    }

    // -- repository API methods --

    public List<ConfigModel> setConfig(ConfigGroupModel groupModel) {
        logger.info("Setting configuration - label: " + groupModel.getLabel());

        // Validate before setting any values
        for (ConfigModel configModel : groupModel.getGroup()) {
            ConfigBase config = lookup(configModel.getName());
            config.validate(configModel);
        }

        List<ConfigModel> priorConfigList = new LinkedList<>();
        for (ConfigModel configModel : groupModel.getGroup()) {
            ConfigEnum configEnum = ConfigEnum.lookupByApiName(configModel.getName());
            ConfigBase config = configuration.get(configEnum);
            ConfigModel prior = config.set(configModel);
            priorConfigList.add(prior);
        }

        return priorConfigList;
    }

    public ConfigModel getConfig(String name) {
        ConfigBase config = lookup(name);
        return config.get();
    }

    public List<ConfigModel> getConfigList() {
        List<ConfigModel> configList = new LinkedList<>();
        for (ConfigBase config : configuration.values()) {
            configList.add(config.get());
        }
        return configList;
    }

    public void reset() {
        for (ConfigBase config : configuration.values()) {
            config.reset();
        }
    }

    public void setFault(String name, boolean enable) {
        ConfigBase configBase = lookup(name);
        configBase.validateType(ConfigModel.ConfigTypeEnum.FAULT);

        ConfigFault fault = (ConfigFault)configBase;
        fault.setEnabled(enable);
    }

    // Exposed for use in unit test
    <T> void addParameter(ConfigEnum configEnum, T value) {
        checkDuplicate(configEnum);
        ConfigParameter param = new ConfigParameter(configEnum, value);
        configuration.put(configEnum, param);
    }

    public <T> T getParameterValue(ConfigEnum configEnum) {
        return lookupByEnum(configEnum).getCurrentValue();
    }

    void addFaultCounted(ConfigEnum configEnum,
                         int skipFor,
                         int insert,
                         int rate,
                         ConfigFaultCountedModel.RateStyleEnum rateStyle) {
        checkDuplicate(configEnum);
        ConfigFaultCountedModel countedModel = new ConfigFaultCountedModel()
            .skipFor(skipFor)
            .insert(insert)
            .rate(rate)
            .rateStyle(rateStyle);
        ConfigFaultCounted fault = new ConfigFaultCounted(configEnum,
            ConfigFaultModel.FaultTypeEnum.COUNTED,
            false,
            countedModel);
        configuration.put(configEnum, fault);
    }

    void addFaultSimple(ConfigEnum configEnum) {
        checkDuplicate(configEnum);
        ConfigFaultSimple fault = new ConfigFaultSimple(configEnum, ConfigFaultModel.FaultTypeEnum.SIMPLE, false);
        configuration.put(configEnum, fault);
    }

    public boolean testInsertFault(ConfigEnum configEnum) {
        ConfigBase configBase = lookupByEnum(configEnum);
        configBase.validateType(ConfigModel.ConfigTypeEnum.FAULT);
        ConfigFault fault = (ConfigFault)configBase;
        return fault.testInsertFault();
    }

    private ConfigBase lookup(String name) {
        ConfigEnum configEnum = ConfigEnum.lookupByApiName(name);
        return lookupByEnum(configEnum);
    }

    private ConfigBase lookupByEnum(ConfigEnum configEnum) {
        ConfigBase config = configuration.get(configEnum);
        if (config == null) {
            throw new ConfigNotFoundException("Unknown configuration name: " + configEnum.name());
        }
        return config;
    }

    private void checkDuplicate(ConfigEnum configEnum) {
        if (configuration.containsKey(configEnum)) {
            throw new DuplicateConfigNameException("Duplicate config name: " + configEnum.name());
        }
    }

    // -- Configuration Setup --

    // Setup the configuration. This is done once during construction.
    private void setConfiguration() {
        addParameter(SAM_RETRY_INITIAL_WAIT_SECONDS, samConfiguration.getRetryInitialWaitSeconds());
        addParameter(SAM_RETRY_MAXIMUM_WAIT_SECONDS, samConfiguration.getRetryMaximumWaitSeconds());
        addParameter(SAM_OPERATION_TIMEOUT_SECONDS, samConfiguration.getOperationTimeoutSeconds());
    }





}

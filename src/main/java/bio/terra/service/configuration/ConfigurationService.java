package bio.terra.service.configuration;

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

@Component
public class ConfigurationService {
    private final Logger logger = LoggerFactory.getLogger(ConfigurationService.class);

    private final SamConfiguration samConfiguration;

    private Map<String, ConfigBase> configuration = new HashMap<>();

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
            ConfigBase config = configuration.get(configModel.getName());
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

    // Exposed for use in unit test
    <T> void addParameter(String name, T value) {
        if (configuration.containsKey(name)) {
            throw new DuplicateConfigNameException("Duplicate config name: " + name);
        }

        ConfigParameter param = new ConfigParameter(name, value);
        configuration.put(param.getName(), param);
    }

    public <T> T getParameterValue(String name) {
        return lookup(name).getCurrentValue();
    }

    private ConfigBase lookup(String name) {
        ConfigBase config = configuration.get(name);
        if (config == null) {
            throw new ConfigNotFoundException("Unknown configuration name: " + name);
        }
        return config;
    }

    // -- Configuration Setup --
    public static final String SAM_RETRY_INITIAL_WAIT_SECONDS = "param.sam.retryInitialWaitSeconds";
    public static final String SAM_RETRY_MAXIMUM_WAIT_SECONDS = "param.sam.retryMaximumWaitSeconds";
    public static final String SAM_OPERATION_TIMEOUT_SECONDS = "param.sam.operationTimeoutSeconds";

    // Setup the configuration. This is done once during construction.
    private void setConfiguration() {
        addParameter(SAM_RETRY_INITIAL_WAIT_SECONDS, samConfiguration.getRetryInitialWaitSeconds());
        addParameter(SAM_RETRY_MAXIMUM_WAIT_SECONDS, samConfiguration.getRetryMaximumWaitSeconds());
        addParameter(SAM_OPERATION_TIMEOUT_SECONDS, samConfiguration.getOperationTimeoutSeconds());
    }





}

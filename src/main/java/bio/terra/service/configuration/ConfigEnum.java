package bio.terra.service.configuration;

import bio.terra.service.configuration.exception.ConfigNotFoundException;
import org.apache.commons.lang3.StringUtils;

// TODO: when we move to OpenAPI V3, we can put this an enum in the swagger and get it out of here.
public enum ConfigEnum {
    SAM_RETRY_INITIAL_WAIT_SECONDS,
    SAM_RETRY_MAXIMUM_WAIT_SECONDS,
    SAM_OPERATION_TIMEOUT_SECONDS;

    public static ConfigEnum lookupByApiName(String apiName) {
        for (ConfigEnum config : values()) {
            if (StringUtils.equalsIgnoreCase(config.name(), apiName)) {
                return config;
            }
        }
        throw new ConfigNotFoundException("Configuration '" + apiName + "' was not found");
    }
}

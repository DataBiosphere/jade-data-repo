package bio.terra.service.configuration;

import bio.terra.service.configuration.exception.ConfigNotFoundException;
import org.apache.commons.lang3.StringUtils;


/**
 * NOTE: the string form of the enumerations are used in tests. A simple IntelliJ rename will not work properly.
 */
// TODO: when we move to OpenAPI V3, we can put this an enum in the swagger and use the enums in the caller
public enum ConfigEnum {
    // -- parameters --
    SAM_RETRY_INITIAL_WAIT_SECONDS,
    SAM_RETRY_MAXIMUM_WAIT_SECONDS,
    SAM_OPERATION_TIMEOUT_SECONDS,

    // -- faults --
    SAM_TIMEOUT_FAULT,
    CREATE_ASSET_FAULT,
    // TODO: When we do DR-737 and attach data to faults, these two can be combined into one.
    LOAD_LOCK_CONFLICT_STOP_FAULT,
    LOAD_LOCK_CONFLICT_CONTINUE_FAULT,

    // Faults to test the fault system
    UNIT_TEST_SIMPLE_FAULT,
    UNIT_TEST_COUNTED_FAULT;

    public static ConfigEnum lookupByApiName(String apiName) {
        for (ConfigEnum config : values()) {
            if (StringUtils.equalsIgnoreCase(config.name(), apiName)) {
                return config;
            }
        }
        throw new ConfigNotFoundException("Configuration '" + apiName + "' was not found");
    }
}

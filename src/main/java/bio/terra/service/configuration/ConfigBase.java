package bio.terra.service.configuration;

import bio.terra.model.ConfigModel;
import bio.terra.service.configuration.exception.InvalidConfigTypeException;

public abstract class ConfigBase {
    private final ConfigModel.ConfigTypeEnum configType;
    private final ConfigEnum configEnum;

    ConfigBase(ConfigModel.ConfigTypeEnum configType, ConfigEnum configEnum) {
        this.configType = configType;
        this.configEnum = configEnum;
    }

    public ConfigModel.ConfigTypeEnum getConfigType() {
        return configType;
    }

    public ConfigEnum getConfigEnum() {
        return configEnum;
    }

    public String getName() {
        return configEnum.name();
    }

    public <T> T getCurrentValue() {
        throw new InvalidConfigTypeException("Config is not a parameter: " + configEnum.name());
    }

    abstract void reset();

    abstract ConfigModel set(ConfigModel configModel);

    abstract void validate(ConfigModel configModel);

    abstract ConfigModel get();
}

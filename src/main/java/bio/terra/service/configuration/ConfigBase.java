package bio.terra.service.configuration;

import bio.terra.model.ConfigModel;
import bio.terra.service.configuration.exception.InvalidConfigTypeException;

public abstract class ConfigBase {
    private final ConfigModel.ConfigTypeEnum configType;
    private final String name;

    ConfigBase(ConfigModel.ConfigTypeEnum configType, String name) {
        this.configType = configType;
        this.name = name;
    }

    public ConfigModel.ConfigTypeEnum getConfigType() {
        return configType;
    }

    public String getName() {
        return name;
    }

    public <T> T getCurrentValue() {
        throw new InvalidConfigTypeException("Config is not a parameter: " + name);
    }

    abstract void reset();

    abstract ConfigModel set(ConfigModel configModel);

    abstract void validate(ConfigModel configModel);

    abstract ConfigModel get();
}

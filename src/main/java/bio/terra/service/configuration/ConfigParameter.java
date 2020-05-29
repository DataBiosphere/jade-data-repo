package bio.terra.service.configuration;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.model.ConfigModel;
import bio.terra.model.ConfigParameterModel;
import bio.terra.service.configuration.exception.UnsupportedConfigDatatypeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigParameter<T> extends ConfigBase {
    private final Logger logger = LoggerFactory.getLogger(ConfigParameter.class);

    private final T originalValue;
    private T currentValue;
    private T scaledValue;

    // -- Instance methods --
    ConfigParameter(ConfigEnum configEnum, T value) {
        super(ConfigModel.ConfigTypeEnum.PARAMETER, configEnum);
        this.originalValue = value;
        this.currentValue = value;
        this.scaledValue = value;
    }

    @Override
    public T getCurrentValue() {
        return currentValue;
    }

    @Override
    public T getScaledValue() {
       //TODO get pod count
        // scaled val = pod count * current value
        // Right now, I'm getting the pod count in the Driver step
        // Seems like it would be to much to wire up to kubeservice here
        scaledValue = currentValue;
        return scaledValue;
    }

    @Override
    void reset() {
        currentValue = originalValue;
    }

    @Override
    ConfigModel get() {
        return new ConfigModel()
            .configType(ConfigModel.ConfigTypeEnum.PARAMETER)
            .name(getName())
            .parameter(new ConfigParameterModel().value(currentValue.toString()));
    }

    @Override
    ConfigModel set(ConfigModel configModel) {
        String newValue = configModel.getParameter().getValue();
        ConfigModel prior = get();
        logger.info("Setting parameter " + getName() + ": prior=" + currentValue + "; new=" + newValue);
        setNewValue(newValue);
        return prior;
    }

    @Override
    void validate(ConfigModel configModel) {
        if (configModel.getConfigType() != ConfigModel.ConfigTypeEnum.PARAMETER) {
            throw new ValidationException("Mismatched config: " + getName() +
                " is a PARAMETER; request is a " + configModel.getConfigType().name());
        }

        if (configModel.getParameter() == null) {
            throw new ValidationException("ConfigParameterModel must be specified");
        }
    }

    // REVIEWERS: feel free to suggest an alternate implementation.
    // It didn't seem worth subclassing, just to call the right valueOf.
    private void setNewValue(String newValue) {
        if (originalValue instanceof String) {
            currentValue = (T)newValue;
        } else if (originalValue instanceof Long) {
            currentValue = (T)Long.valueOf(newValue);
        } else if (originalValue instanceof  Integer) {
            currentValue = (T)Integer.valueOf(newValue);
        } else if (originalValue instanceof Double) {
            currentValue = (T)Double.valueOf(newValue);
        } else if (originalValue instanceof Float) {
            currentValue = (T)Float.valueOf(newValue);
        } else {
            throw new UnsupportedConfigDatatypeException("Unsupported datatype: " +
                originalValue.getClass().getSimpleName());
        }
    }
}

package bio.terra.service.configuration;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.model.ConfigFaultModel;
import bio.terra.model.ConfigModel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class ConfigFault extends ConfigBase {
  private final Logger logger = LoggerFactory.getLogger(ConfigFault.class);

  private final ConfigFaultModel.FaultTypeEnum faultType;
  private final boolean originalEnabled;
  private boolean enabled;

  // -- Instance methods --
  ConfigFault(ConfigEnum configEnum, ConfigFaultModel.FaultTypeEnum faultType, boolean enabled) {
    super(ConfigModel.ConfigTypeEnum.FAULT, configEnum);
    this.faultType = faultType;
    this.originalEnabled = enabled;
    this.enabled = enabled;
  }

  @Override
  synchronized void reset() {
    enabled = originalEnabled;
    resetFaultConfig();
  }

  boolean isEnabled() {
    return enabled;
  }

  synchronized void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  @Override
  synchronized ConfigModel set(ConfigModel configModel) {
    validate(configModel);
    ConfigModel prior = get();

    ConfigFaultModel faultModel = configModel.getFault();
    setFaultConfig(faultModel);

    // Set the enabled last so the common case of
    logger.info(
        "Setting fault " + getName() + ": from=" + isEnabled() + "; new=" + faultModel.isEnabled());
    enabled = faultModel.isEnabled();
    return prior;
  }

  @Override
  void validate(ConfigModel configModel) {
    super.validateType(ConfigModel.ConfigTypeEnum.FAULT);

    if (configModel.getFault() == null) {
      throw new ValidationException("ConfigFaultModel must be specified");
    }

    if (configModel.getFault().getFaultType() != faultType) {
      throw new ValidationException(
          "Mismatched fault type: "
              + getName()
              + " is a FAULT of type "
              + faultType
              + "; request is of type "
              + configModel.getFault().getFaultType().name());
    }
  }

  abstract boolean testInsertFault();

  abstract void setFaultConfig(ConfigFaultModel faultModel);

  abstract void resetFaultConfig();
}

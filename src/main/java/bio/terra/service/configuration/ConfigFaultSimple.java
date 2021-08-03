package bio.terra.service.configuration;

import bio.terra.model.ConfigFaultModel;
import bio.terra.model.ConfigModel;

public class ConfigFaultSimple extends ConfigFault {
  ConfigFaultSimple(
      ConfigEnum configEnum, ConfigFaultModel.FaultTypeEnum faultType, boolean enabled) {
    super(configEnum, faultType, enabled);
  }

  @Override
  ConfigModel get() {
    ConfigFaultModel faultSimple =
        new ConfigFaultModel()
            .faultType(ConfigFaultModel.FaultTypeEnum.SIMPLE)
            .enabled(isEnabled());

    return new ConfigModel()
        .configType(ConfigModel.ConfigTypeEnum.FAULT)
        .name(getName())
        .fault(faultSimple);
  }

  // For the simple case, there is no fault configuration beyond enabled,
  // so there is not much to do for set or reset

  @Override
  void setFaultConfig(ConfigFaultModel faultModel) {}

  @Override
  void resetFaultConfig() {}

  @Override
  boolean testInsertFault() {
    return isEnabled();
  }
}

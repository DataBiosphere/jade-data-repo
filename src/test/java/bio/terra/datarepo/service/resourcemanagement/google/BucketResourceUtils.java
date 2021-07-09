package bio.terra.datarepo.service.resourcemanagement.google;

import bio.terra.datarepo.model.ConfigGroupModel;
import bio.terra.datarepo.model.ConfigModel;
import bio.terra.datarepo.model.ConfigParameterModel;
import bio.terra.datarepo.service.configuration.ConfigEnum;
import bio.terra.datarepo.service.configuration.ConfigurationService;

public class BucketResourceUtils {
  boolean getAllowReuseExistingBuckets(ConfigurationService configService) {
    return configService.getParameterValue(ConfigEnum.ALLOW_REUSE_EXISTING_BUCKETS);
  }

  public void setAllowReuseExistingBuckets(ConfigurationService configService, boolean allow) {
    ConfigModel model = configService.getConfig(ConfigEnum.ALLOW_REUSE_EXISTING_BUCKETS.name());
    model.setParameter(new ConfigParameterModel().value(String.valueOf(allow)));
    ConfigGroupModel configGroupModel =
        new ConfigGroupModel().label("BucketResourceTest").addGroupItem(model);
    configService.setConfig(configGroupModel);
  }
}

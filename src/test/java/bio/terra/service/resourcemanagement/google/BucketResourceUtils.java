package bio.terra.service.resourcemanagement.google;

import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.model.ConfigParameterModel;
import bio.terra.model.ConfigModel;
import bio.terra.model.ConfigGroupModel;

public class BucketResourceUtils {
    boolean getAllowReuseExistingBuckets(ConfigurationService configService) {
        return configService.getParameterValue(ConfigEnum.ALLOW_REUSE_EXISTING_BUCKETS);
    }

    public void setAllowReuseExistingBuckets(ConfigurationService configService, boolean allow) {
        ConfigModel model = configService.getConfig(ConfigEnum.ALLOW_REUSE_EXISTING_BUCKETS.name());
        model.setParameter(new ConfigParameterModel().value(String.valueOf(allow)));
        ConfigGroupModel configGroupModel = new ConfigGroupModel()
            .label("BucketResourceTest")
            .addGroupItem(model);
        configService.setConfig(configGroupModel);
    }
}

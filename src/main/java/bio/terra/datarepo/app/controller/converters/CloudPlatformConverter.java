package bio.terra.datarepo.app.controller.converters;

import bio.terra.datarepo.model.CloudPlatform;
import java.util.Arrays;
import org.springframework.stereotype.Component;

@Component
public class CloudPlatformConverter extends OpenApiEnumConverter<CloudPlatform> {

  @Override
  CloudPlatform fromValue(String source) {
    return CloudPlatform.fromValue(source);
  }

  @Override
  String errorString() {
    return String.format(
        "cloudPlatform must be one of: %s.", Arrays.toString(CloudPlatform.values()));
  }
}

package bio.terra.app.controller.converters;

import bio.terra.model.CloudPlatform;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class CloudPlatformConverter extends OpenApiEnumConverter<CloudPlatform> {

    @Override
    CloudPlatform fromValue(String source) {
        return CloudPlatform.fromValue(source);
    }

    @Override
    String errorString() {
        return String.format("cloudPlatform must be one of: %s.", Arrays.toString(CloudPlatform.values()));
    }
}

package bio.terra.app.controller.converters;

import bio.terra.app.model.GoogleRegion;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class GoogleRegionConverter extends OpenApiEnumConverter<GoogleRegion> {

    @Override
    GoogleRegion fromValue(String source) {
        return GoogleRegion.fromValue(source);
    }

    @Override
    String errorString() {
        return String.format("region must be one of: %s.", Arrays.toString(GoogleRegion.values()));
    }
}

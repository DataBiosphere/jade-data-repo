package bio.terra.app.controller.converters;

import bio.terra.model.EnumerateSortByParam;
import org.springframework.stereotype.Component;

import java.util.Arrays;

@Component
public class EnumerateSortByParamConverter extends OpenApiEnumConverter<EnumerateSortByParam> {

    @Override
    EnumerateSortByParam fromValue(String source) {
        return EnumerateSortByParam.fromValue(source);
    }

    @Override
    String errorString() {
        return String.format("sort must be one of: %s.", Arrays.toString(EnumerateSortByParam.values()));
    }
}

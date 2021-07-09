package bio.terra.datarepo.app.controller.converters;

import bio.terra.datarepo.model.EnumerateSortByParam;
import java.util.Arrays;
import org.springframework.stereotype.Component;

@Component
public class EnumerateSortByParamConverter extends OpenApiEnumConverter<EnumerateSortByParam> {

  @Override
  EnumerateSortByParam fromValue(String source) {
    return EnumerateSortByParam.fromValue(source);
  }

  @Override
  String errorString() {
    return String.format(
        "sort must be one of: %s.", Arrays.toString(EnumerateSortByParam.values()));
  }
}

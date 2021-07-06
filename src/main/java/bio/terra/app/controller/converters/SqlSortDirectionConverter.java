package bio.terra.app.controller.converters;

import bio.terra.model.SqlSortDirection;
import java.util.Arrays;
import org.springframework.stereotype.Component;

@Component
public class SqlSortDirectionConverter extends OpenApiEnumConverter<SqlSortDirection> {

  @Override
  SqlSortDirection fromValue(String source) {
    return SqlSortDirection.fromValue(source);
  }

  @Override
  String errorString() {
    return String.format(
        "direction must be one of: %s.", Arrays.toString(SqlSortDirection.values()));
  }
}

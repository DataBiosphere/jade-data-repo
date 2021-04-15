package bio.terra.app.controller.converters;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.model.SqlSortDirection;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;
import java.util.stream.Collectors;

@Component
public class SqlSortDirectionConverter
    implements Converter<String, SqlSortDirection> {

    @Override
    public SqlSortDirection convert(String source) {
        SqlSortDirection result = SqlSortDirection.fromValue(source.toLowerCase());
        if (result == null) {
            String error = String.format("direction must be one of: (%s).",
                Arrays.stream(SqlSortDirection.values())
                    .map(SqlSortDirection::toString)
                    .collect(Collectors.joining(", ")));
            throw new ValidationException("Invalid enumerate parameter(s).", Collections.singletonList(error));
        }
        return result;
    }
}

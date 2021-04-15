package bio.terra.app.controller.converters;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.model.EnumerateSortByParam;

import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;

@Component
public class EnumerateSortByParamConverter
    implements Converter<String, EnumerateSortByParam> {

    @Override
    public EnumerateSortByParam convert(String source) {
        EnumerateSortByParam result = EnumerateSortByParam.fromValue(source.toLowerCase());
        if (result == null) {
            String error = String.format("sort must be one of: %s.", Arrays.toString(EnumerateSortByParam.values()));
            throw new ValidationException("Invalid enumerate parameter(s).", Collections.singletonList(error));
        }
        return result;    }
}

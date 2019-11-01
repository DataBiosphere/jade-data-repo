package bio.terra.service.iam;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

public enum IamRole {
    ADMIN,
    STEWARD,
    CUSTODIAN,
    INGESTER,
    READER,
    DISCOVERER;

    @Override
    @JsonValue
    public String toString() {
        return StringUtils.lowerCase(name());
    }

    @JsonCreator
    public static IamRole fromValue(String text) {
        for (IamRole b : IamRole.values()) {
            if (String.valueOf(b.name()).equals(StringUtils.upperCase(text))) {
                return b;
            }
        }
        return null;
    }
}


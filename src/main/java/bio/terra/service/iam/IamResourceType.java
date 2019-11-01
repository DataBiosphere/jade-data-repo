package bio.terra.service.iam;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

public enum IamResourceType {
    DATAREPO,
    DATASET,
    DATASNAPSHOT;

    @Override
    @JsonValue
    public String toString() {
        return StringUtils.lowerCase(name());
    }

    @JsonCreator
    static IamResourceType fromValue(String text) {
        for (IamResourceType b : IamResourceType.values()) {
            if (b.name().equals(StringUtils.upperCase(text))) {
                return b;
            }
        }
        return null;
    }
}

package bio.terra.service.iam;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

// NOTE: these action enums must have exactly the same text as the Sam action name.
public enum IamAction {
    // common
    CREATE,
    DELETE,
    SHARE_POLICY,
    READ_POLICY,
    READ_POLICIES,
    ALTER_POLICIES,
    // datarepo
    CREATE_DATASET,
    LIST_JOBS,
    DELETE_JOBS,
    // dataset
    EDIT_DATASET,
    READ_DATASET,
    INGEST_DATA,
    UPDATE_DATA,
    // snapshots
    CREATE_DATASNAPSHOT,
    EDIT_DATASNAPSHOT,
    READ_DATA,
    DISCOVER_DATA,
    // billing profiles
    UPDATE_BILLING_ACCOUNT,
    LINK;


    @Override
    @JsonValue
    public String toString() {
        return StringUtils.lowerCase(name());
    }

    @JsonCreator
    public static IamAction fromValue(String text) {
        for (IamAction b : IamAction.values()) {
            if (String.valueOf(b.name()).equals(StringUtils.upperCase(text))) {
                return b;
            }
        }
        return null;
    }
}

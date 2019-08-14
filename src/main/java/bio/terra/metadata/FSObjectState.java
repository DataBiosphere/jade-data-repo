package bio.terra.metadata;

import bio.terra.filesystem.exception.InvalidFileSystemObjectTypeException;
import org.apache.commons.lang3.StringUtils;

/**
 * Describes the state of the object in Firestore
 */
public enum FSObjectState {
    EXISTING_FILE("F"),
    INGESTING_FILE("N"),
    DELETING_FILE("X");

    private String letter;

    FSObjectState(String letter) {
        this.letter = letter;
    }

    public static FSObjectState fromLetter(String match) {
        for (FSObjectState test : FSObjectState.values()) {
            if (StringUtils.equals(test.letter, match)) {
                return test;
            }
        }
        throw new InvalidFileSystemObjectTypeException("Invalid object state: '" + match + "'");
    }

    public String toLetter() {
        return letter;
    }
}


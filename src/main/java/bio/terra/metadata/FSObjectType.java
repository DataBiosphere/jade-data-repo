package bio.terra.metadata;

import bio.terra.filesystem.exception.InvalidFileSystemObjectTypeException;
import org.apache.commons.lang3.StringUtils;

/**
 * Describes the state of the object in Firestore
 */
public enum FSObjectType {
    DIRECTORY("D"),
    FILE("F"),
    INGESTING_FILE("N"),
    DELETING_FILE("X");

    private String letter;

    FSObjectType(String letter) {
        this.letter = letter;
    }

    public static FSObjectType fromLetter(String match) {
        for (FSObjectType test : FSObjectType.values()) {
            if (StringUtils.equals(test.letter, match)) {
                return test;
            }
        }
        throw new InvalidFileSystemObjectTypeException("Invalid object type: '" + match + "'");
    }

    public String toLetter() {
        return letter;
    }
}


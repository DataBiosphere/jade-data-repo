package bio.terra.metadata;

import bio.terra.dao.exception.InvalidFileSystemObjectTypeException;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.UUID;

public class FSObject {
    public enum FSObjectType {
        DIRECTORY("D"),
        FILE("F"),
        FILE_NOT_PRESENT("N");

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

    private UUID objectId;
    private UUID studyId;
    private FSObjectType objectType;
    private Instant createdDate;
    private String path;
    private String gspath;
    private String checksum;
    private Long size;
    private String mimeType;
    private String description;
    private String creatingFlightId;

    public UUID getObjectId() {
        return objectId;
    }

    public FSObject objectId(UUID objectId) {
        this.objectId = objectId;
        return this;
    }

    public UUID getStudyId() {
        return studyId;
    }

    public FSObject studyId(UUID studyId) {
        this.studyId = studyId;
        return this;
    }

    public FSObjectType getObjectType() {
        return objectType;
    }

    public FSObject objectType(FSObjectType objectType) {
        this.objectType = objectType;
        return this;
    }

    public Instant getCreatedDate() {
        return createdDate;
    }

    public FSObject createdDate(Instant createdDate) {
        this.createdDate = createdDate;
        return this;
    }

    public String getPath() {
        return path;
    }

    public FSObject path(String path) {
        this.path = path;
        return this;
    }

    public String getGspath() {
        return gspath;
    }

    public FSObject gspath(String gspath) {
        this.gspath = gspath;
        return this;
    }

    public String getChecksum() {
        return checksum;
    }

    public FSObject checksum(String checksum) {
        this.checksum = checksum;
        return this;
    }

    public Long getSize() {
        return size;
    }

    public FSObject size(Long size) {
        this.size = size;
        return this;
    }

    public String getMimeType() {
        return mimeType;
    }

    public FSObject mimeType(String mimeType) {
        this.mimeType = mimeType;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public FSObject description(String description) {
        this.description = description;
        return this;
    }

    public String getCreatingFlightId() {
        return creatingFlightId;
    }

    public FSObject creatingFlightId(String creatingFlightId) {
        this.creatingFlightId = creatingFlightId;
        return this;
    }
}

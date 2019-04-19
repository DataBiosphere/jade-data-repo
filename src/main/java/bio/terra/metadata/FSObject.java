package bio.terra.metadata;

import bio.terra.filesystem.exception.InvalidFileSystemObjectTypeException;
import org.apache.commons.lang3.StringUtils;

import java.time.Instant;
import java.util.UUID;

public class FSObject {
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

    private UUID objectId;
    private UUID studyId;
    private FSObjectType objectType;
    private Instant createdDate;
    private String path;
    private String gspath;
    private String checksumCrc32c;
    private String checksumMd5;  // may be null if it can't be used to validate the file contents
    private Long size;
    private String mimeType;
    private String description;
    private String flightId;

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

    public String getChecksumCrc32c() {
        return checksumCrc32c;
    }

    public FSObject checksumCrc32c(String checksumCrc32c) {
        this.checksumCrc32c = checksumCrc32c;
        return this;
    }

    public String getChecksumMd5() {
        return checksumMd5;
    }

    public FSObject checksumMd5(String checksumMd5) {
        this.checksumMd5 = checksumMd5;
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

    public String getFlightId() {
        return flightId;
    }

    public FSObject flightId(String flightId) {
        this.flightId = flightId;
        return this;
    }
}

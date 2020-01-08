package bio.terra.service.load;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.UUID;

public class LoadFile {
    public enum State {
        NOT_TRIED,
        SUCCEEDED,
        FAILED
    }

    private UUID loadId;
    private String sourcePath;
    private String targetPath;
    private LoadFile.State state;
    private String fileId;
    private String error;

    public UUID getLoadId() {
        return loadId;
    }

    public LoadFile loadId(UUID loadId) {
        this.loadId = loadId;
        return this;
    }

    public String getSourcePath() {
        return sourcePath;
    }

    public LoadFile sourcePath(String sourcePath) {
        this.sourcePath = sourcePath;
        return this;
    }

    public String getTargetPath() {
        return targetPath;
    }

    public LoadFile targetPath(String targetPath) {
        this.targetPath = targetPath;
        return this;
    }

    public State getState() {
        return state;
    }

    public LoadFile state(State state) {
        this.state = state;
        return this;
    }

    public String getFileId() {
        return fileId;
    }

    public LoadFile fileId(String fileId) {
        this.fileId = fileId;
        return this;
    }

    public String getError() {
        return error;
    }

    public LoadFile error(String error) {
        this.error = error;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
            .append("loadId", loadId)
            .append("sourcePath", sourcePath)
            .append("targetPath", targetPath)
            .append("state", state)
            .append("fileId", fileId)
            .append("error", error)
            .toString();
    }
}

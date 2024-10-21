package bio.terra.service.load;

import bio.terra.model.BulkLoadFileState;
import java.util.UUID;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

public class LoadFile {
  private UUID loadId;
  private String sourcePath;
  private String targetPath;
  private String mimeType;
  private String description;
  private BulkLoadFileState state;
  private String flightId;
  private String fileId;
  private String error;
  private String md5;

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

  public String getMimeType() {
    return mimeType;
  }

  public LoadFile mimeType(String mimeType) {
    this.mimeType = mimeType;
    return this;
  }

  public String getDescription() {
    return description;
  }

  public LoadFile description(String description) {
    this.description = description;
    return this;
  }

  public BulkLoadFileState getState() {
    return state;
  }

  public LoadFile state(BulkLoadFileState state) {
    this.state = state;
    return this;
  }

  public String getFlightId() {
    return flightId;
  }

  public LoadFile flightId(String flightId) {
    this.flightId = flightId;
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

  public String getMd5() {
    return md5;
  }

  public LoadFile md5(String md5) {
    this.md5 = md5;
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
        .append("md5", md5)
        .toString();
  }
}

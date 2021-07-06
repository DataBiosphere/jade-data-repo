package bio.terra.service.load;

import java.util.List;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Returned from the DAO, it includes the current number of failed file loads and K candidate files
 * that have been NOT_TRIED.
 */
public class LoadCandidates {
  private int failedLoads;
  private List<LoadFile> runningLoads;
  private List<LoadFile> candidateFiles;

  public int getFailedLoads() {
    return failedLoads;
  }

  public LoadCandidates failedLoads(int failedLoads) {
    this.failedLoads = failedLoads;
    return this;
  }

  public List<LoadFile> getRunningLoads() {
    return runningLoads;
  }

  public LoadCandidates runningLoads(List<LoadFile> runningLoads) {
    this.runningLoads = runningLoads;
    return this;
  }

  public List<LoadFile> getCandidateFiles() {
    return candidateFiles;
  }

  public LoadCandidates candidateFiles(List<LoadFile> candidateFiles) {
    this.candidateFiles = candidateFiles;
    return this;
  }

  @Override
  public String toString() {
    return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
        .append("failedLoads", failedLoads)
        .append("candidateFiles", candidateFiles)
        .toString();
  }
}

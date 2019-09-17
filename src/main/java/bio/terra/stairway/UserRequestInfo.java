package bio.terra.stairway;


import java.util.UUID;

public class UserRequestInfo {
    private String name;
    private String subjectId;
    private UUID requestId;
    private boolean canListJobs;
    private boolean canDeleteJobs;

    public String getName() {
        return name;
    }

    public UserRequestInfo name(String name) {
        this.name = name;
        return this;
    }

    public String getSubjectId() {
        return subjectId;
    }

    public UserRequestInfo subjectId(String subjectId) {
        this.subjectId = subjectId;
        return this;
    }

    public UUID getRequestId() {
        return requestId;
    }

    public UserRequestInfo requestId(UUID reqId) {
        this.requestId = reqId;
        return this;
    }

    public boolean getCanListJobs() {
        return canListJobs;
    }

    public UserRequestInfo canListJobs(boolean canListJobs) {
        this.canListJobs = canListJobs;
        return this;
    }

    public boolean getCanDeleteJobs() {
        return canDeleteJobs;
    }

    public UserRequestInfo canDeleteJobs(boolean canDeleteJobs) {
        this.canDeleteJobs = canDeleteJobs;
        return this;
    }

}

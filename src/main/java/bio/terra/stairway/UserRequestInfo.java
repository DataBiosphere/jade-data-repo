package bio.terra.stairway;


import java.util.UUID;

public class UserRequestInfo {
    private String name;
    private String subjectId;
    private UUID requestId;

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

}

package bio.terra.filesystem;

import com.google.auth.Credentials;

public class ProjectAndCredential {
    private String projectId;
    private Credentials credentials;

    public ProjectAndCredential(String projectId, Credentials credentials) {
        this.credentials = credentials;
        this.projectId = projectId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ProjectAndCredential)) return false;
        ProjectAndCredential projectAndCredential = (ProjectAndCredential) o;
        return projectAndCredential.credentials == this.credentials
            && projectAndCredential.projectId.equals(this.projectId);
    }

    @Override
    public int hashCode() {
        int result = projectId.hashCode();

        if (credentials != null) {
            result += credentials.hashCode() * 31;
        }

        return result;
    }
}

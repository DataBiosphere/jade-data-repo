package bio.terra.common.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

@Component
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "it")
public class TestConfiguration {
    private String jadeApiUrl;
    private String jadePemFileName;
    private String jadeEmail;
    private String ingestbucket;
    private List<User> users = new ArrayList<>();
    private String googleProjectId;
    private String googleBillingAccountId;

    public static class User {
        private String role;
        private String name;
        private String email;
        private String subjectId;

        public String getRole() {
            return role;
        }

        public void setRole(String role) {
            this.role = role;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public String getSubjectId() {
            return subjectId;
        }

        public void setSubjectId(String subjectId) {
            this.subjectId = subjectId;
        }
    }

    public String getJadeApiUrl() {
        return jadeApiUrl;
    }

    public void setJadeApiUrl(String jadeApiUrl) {
        this.jadeApiUrl = jadeApiUrl;
    }

    public String getJadePemFileName() {
        return jadePemFileName;
    }

    public void setJadePemFileName(String jadePemFileName) {
        this.jadePemFileName = jadePemFileName;
    }

    public String getJadeEmail() {
        return jadeEmail;
    }

    public void setJadeEmail(String jadeSAEmail) {
        this.jadeEmail = jadeSAEmail;
    }

    public String getIngestbucket() {
        return ingestbucket;
    }

    public void setIngestbucket(String ingestbucket) {
        this.ingestbucket = ingestbucket;
    }

    public List<User> getUsers() {
        return users;
    }

    public void setUsers(List<User> users) {
        this.users = users;
    }

    public String getGoogleProjectId() {
        return googleProjectId;
    }

    public void setGoogleProjectId(String googleProjectId) {
        this.googleProjectId = googleProjectId;
    }

    public String getGoogleBillingAccountId() {
        return googleBillingAccountId;
    }

    public void setGoogleBillingAccountId(String googleBillingAccountId) {
        this.googleBillingAccountId = googleBillingAccountId;
    }
}

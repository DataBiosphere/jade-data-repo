package bio.terra.integration.configuration;

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
    private String jadePemFile;
    private String notSoSecretPassword;
    private String ingestbucket;
    private List<User> users = new ArrayList<>();

    public static class User {
        String role;
        String name;
        String email;

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
    }

    public String getJadeApiUrl() {
        return jadeApiUrl;
    }

    public void setJadeApiUrl(String jadeApiUrl) {
        this.jadeApiUrl = jadeApiUrl;
    }

    public String getJadePemFile() {
        return jadePemFile;
    }

    public void setJadePemFile(String jadePemFile) {
        this.jadePemFile = jadePemFile;
    }

    public String getNotSoSecretPassword() {
        return notSoSecretPassword;
    }

    public void setNotSoSecretPassword(String notSoSecretPassword) {
        this.notSoSecretPassword = notSoSecretPassword;
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
}

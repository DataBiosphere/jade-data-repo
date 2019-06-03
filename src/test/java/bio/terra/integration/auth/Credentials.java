package bio.terra.integration.auth;

import bio.terra.integration.configuration.TestConfiguration;

public class Credentials {
    private String email;
    private String password;
    private String name;

    public Credentials(TestConfiguration.User user, String password)  {
        this.email = user.getEmail();
        this.name = user.getName();
        this.password = password;
    }

    public String getEmail() {
        return email;
    }

    public String getPassword() {
        return password;
    }

    public String getName() {
        return name;
    }
}

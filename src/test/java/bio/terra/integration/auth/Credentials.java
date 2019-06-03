package bio.terra.integration.auth;

import bio.terra.integration.configuration.TestConfiguration;

public class Credentials {
    private String email;
    private String name;

    public Credentials(TestConfiguration.User user)  {
        this.email = user.getEmail();
        this.name = user.getName();
    }

    public String getEmail() {
        return email;
    }

    public String getName() {
        return name;
    }
}

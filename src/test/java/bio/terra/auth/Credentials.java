package bio.terra.auth;

import bio.terra.integration.configuration.TestConfiguration;

public class Credentials {
    String email;
    String password;
    String name;
//    AuthToken myToken;

    public Credentials (TestConfiguration.User user, String password)  {
        this.email = user.getEmail();
        this.name = user.getName();
        this.password = password;
    }

//    public AuthToken getAuthToken() {
//        if (myToken == null) // TODO or check for expired or needs refresh
//            myToken = new AuthToken(this);
//        return myToken;
//    }
}

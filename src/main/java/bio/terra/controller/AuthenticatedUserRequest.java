package bio.terra.controller;

import java.util.UUID;

public class AuthenticatedUserRequest {

    private String email;
    private String token;
    private UUID reqId;

    public AuthenticatedUserRequest(String email, String token) {
        this.email = email;
        this.token = token;
        this.reqId = UUID.randomUUID();
    }

    public String getEmail() {
        return email;
    }

    public String getToken() {
        return token;
    }

    public UUID getReqId() {
        return reqId;
    }

}

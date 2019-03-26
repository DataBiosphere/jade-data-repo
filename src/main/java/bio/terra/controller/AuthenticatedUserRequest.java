package bio.terra.controller;

import java.util.UUID;

public class AuthenticatedUserRequest {

    private String email;
    private String token;
    private UUID reqId;

    public AuthenticatedUserRequest() {}

    public AuthenticatedUserRequest(String email, String token) {
        this.email = email;
        this.token = token;
        this.reqId = UUID.randomUUID();
    }

    public String getEmail() {
        return email;
    }

    public AuthenticatedUserRequest email(String email) {
        this.email = email;
        return this;
    }

    public String getToken() {
        return token;
    }

    public AuthenticatedUserRequest token(String token) {
        this.token = token;
        return this;
    }

    public UUID getReqId() {
        return reqId;
    }

    public AuthenticatedUserRequest reqId(UUID reqId) {
        this.reqId = reqId;
        return this;
    }
}

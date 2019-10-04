package bio.terra.app.controller;

import javax.servlet.http.HttpServletRequest;

public interface AuthenticatedUserRequestFactory {

    AuthenticatedUserRequest from(HttpServletRequest servletRequest);

}

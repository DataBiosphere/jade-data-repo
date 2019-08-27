package bio.terra.controller;

import javax.servlet.http.HttpServletRequest;

public interface AuthenticatedUserRequestFactory {

    AuthenticatedUserRequest from(HttpServletRequest servletRequest);

}

package bio.terra.app.configuration;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;
import java.io.IOException;
import java.util.Collection;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

/**
 * Servlet filter to ensure all cookies have SameSite=Lax attribute set. This addresses security
 * vulnerability where JSESSIONID cookie lacks SameSite attribute.
 */
@Component
@Order(Ordered.HIGHEST_PRECEDENCE)
public class SameSiteCookieFilter implements Filter {

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    // No initialization needed
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
      throws IOException, ServletException {

    HttpServletRequest httpRequest = (HttpServletRequest) request;
    HttpServletResponse httpResponse = (HttpServletResponse) response;

    // Wrap the response to intercept cookie headers
    HttpServletResponseWrapper responseWrapper =
        new HttpServletResponseWrapper(httpResponse) {
          @Override
          public void addHeader(String name, String value) {
            if ("Set-Cookie".equalsIgnoreCase(name)) {
              value = addSameSiteAttribute(value);
            }
            super.addHeader(name, value);
          }

          @Override
          public void setHeader(String name, String value) {
            if ("Set-Cookie".equalsIgnoreCase(name)) {
              value = addSameSiteAttribute(value);
            }
            super.setHeader(name, value);
          }
        };

    chain.doFilter(request, responseWrapper);

    // Also check existing headers and modify them
    Collection<String> headerNames = httpResponse.getHeaderNames();
    for (String headerName : headerNames) {
      if ("Set-Cookie".equalsIgnoreCase(headerName)) {
        Collection<String> cookieHeaders = httpResponse.getHeaders(headerName);
        httpResponse.setHeader(headerName, null); // Clear existing
        for (String cookieValue : cookieHeaders) {
          httpResponse.addHeader(headerName, addSameSiteAttribute(cookieValue));
        }
      }
    }
  }

  private String addSameSiteAttribute(String cookieValue) {
    if (cookieValue == null || cookieValue.toLowerCase().contains("samesite=")) {
      // If SameSite is already set, don't modify it
      if (cookieValue != null && cookieValue.toLowerCase().contains("samesite=none")) {
        // Replace SameSite=None with SameSite=Lax
        return cookieValue.replaceAll("(?i)samesite=none", "SameSite=Lax");
      }
      return cookieValue;
    }

    // Add SameSite=Lax to cookies that don't have it
    return cookieValue + "; SameSite=Lax";
  }

  @Override
  public void destroy() {
    // No cleanup needed
  }
}

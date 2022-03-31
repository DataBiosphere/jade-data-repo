package bio.terra.app.controller;

import org.springframework.core.annotation.Order;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;

@org.springframework.web.bind.annotation.ControllerAdvice
@Order(10000)
public class ControllerAdvice {
  @InitBinder
  public void setAllowedFields(WebDataBinder dataBinder) {
    String[] abd = new String[] {"class.*", "Class.*", "*.class.*", "*.Class.*"};
    dataBinder.setDisallowedFields(abd);
  }
}

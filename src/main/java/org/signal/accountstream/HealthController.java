package org.signal.accountstream;

import io.micronaut.http.MediaType;
import io.micronaut.http.annotation.Controller;
import io.micronaut.http.annotation.Get;

@Controller("/health")
public class HealthController {
  @Get(produces = MediaType.TEXT_PLAIN)
  public String get() {
    return "ok\n";
  }
}

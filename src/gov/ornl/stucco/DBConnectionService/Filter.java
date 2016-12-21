package gov.ornl.stucco.DBConnectionService;

import java.io.File;
import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;

import javax.ws.rs.container.ContainerRequestContext;
import javax.ws.rs.container.ContainerResponseContext;
import javax.ws.rs.container.ContainerResponseFilter;

import org.json.JSONObject;

// @Provider
public class Filter implements ContainerResponseFilter {
	// default ui url
	String API_URL = "http://localhost:8000";

  @Override
  public void filter(ContainerRequestContext request, ContainerResponseContext response) throws IOException {
  	String stuccoUiConfigPath = System.getenv("STUCCO_UI_CONFIG");
	  if (stuccoUiConfigPath != null) {
	  	try {
	  		BufferedReader in = new BufferedReader(new FileReader(stuccoUiConfigPath));
        StringBuilder builder = new StringBuilder();
        String eol = System.getProperty("line.separator");
        String line;
        while ((line = in.readLine()) != null) {
          builder.append(line);
          builder.append(eol);
        }

	  		JSONObject stuccoUiConfig = new JSONObject(builder.toString());
	  		JSONObject client = stuccoUiConfig.optJSONObject("client");
	  		if (client != null) {
	  			String apiUrl = client.optString("apiUrl");
	  			API_URL = (apiUrl != null && !apiUrl.equals("")) ? apiUrl : API_URL;
	  		}
			} catch (IOException e) {
				e.printStackTrace();
			} catch (RuntimeException e) {
				e.printStackTrace();
			}
	  } 

    response.getHeaders().add("Access-Control-Allow-Origin", API_URL);
    response.getHeaders().add("Access-Control-Allow-Headers", "origin, content-type, accept, authorization");
    response.getHeaders().add("Access-Control-Allow-Credentials", "true");
    response.getHeaders().add("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS, HEAD");
  }
}
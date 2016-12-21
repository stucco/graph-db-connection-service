package gov.ornl.stucco.DBConnectionService;

import org.glassfish.jersey.server.ResourceConfig;
import org.glassfish.jersey.jdkhttp.JdkHttpServerFactory;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

public class DBConnectionApplication {

    private final static int DEFAULT_API_PORT = 8080;

    public DBConnectionApplication () {
        try {
            createHttpServer(); 
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
        System.out.println("APPLICATION WAS CREATED!!");
    }

    private void createHttpServer() throws IOException {
        DBConnectionSingleton dbSingleton = new DBConnectionSingleton();
        ResourceConfig resConf = new ResourceConfig();
        resConf.register(new DBConnectionResource(dbSingleton));
        resConf.register(new Filter());
        JdkHttpServerFactory.createHttpServer(getURI(), resConf);
    }

    private static URI getURI() {
        return UriBuilder.fromUri("http://" + getHostName() + "/").port(DEFAULT_API_PORT).build(); //TODO: read port from config?
    }

    private static String getHostName() {
        String hostName = System.getenv("STUCCO_HOST_IP");
        hostName = (hostName == null) ? "localhost" : hostName;
        /*
        }
        try {
            hostName = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        */
        return hostName;
    }

    /**
     * The main method for the DBConnection service
     *
     * @param args Command line arguments
     */
    public static void main(String[] args) {
        //Start api server
        new DBConnectionApplication();
    }
}
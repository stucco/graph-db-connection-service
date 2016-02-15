package gov.ornl.stucco.DBConnectionService;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;

import org.glassfish.jersey.server.ResourceConfig;

import java.util.HashSet;
import java.util.Set;

/*
@ApplicationPath("/*")
public class DBConnectionApplication extends Application {

    @Override
    public Set<Class<?>> getClasses() {
        Set<Class<?>> classes = new HashSet<>();
        classes.add(DBConnectionResource.class);
        classes.add(DBConnectionSingletonFeature.class);
        return classes;
    }

}
*/

public class DBConnectionApplication extends ResourceConfig {

    /*Register JAX-RS application components.*/
    public DBConnectionApplication () {
    	System.out.println("APPLICATION WAS CREATED!!");
        register(DBConnectionSingleton.class);
    }
}
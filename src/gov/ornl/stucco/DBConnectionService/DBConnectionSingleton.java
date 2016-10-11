package gov.ornl.stucco.DBConnectionService;

import gov.pnnl.stucco.dbconnect.inmemory.InMemoryDBConnection;
import gov.pnnl.stucco.dbconnect.DBConnectionAlignment;
import gov.pnnl.stucco.dbconnect.DBConnectionFactory;

import javax.inject.Singleton;
import javax.ws.rs.Path;

/**
 * This singleton will maintain a persistent DBConnection object, which will be accessible when servicing requests.
 * NOTE: two environment variable must be defined:
 *       STUCCO_DB_CONFIG=<path/filename.yml>
 *       STUCCO_DB_TYPE= INMEMORY|ORIENTDB|TITAN|NEO4J|POSTGRESQL
 */
@Path("/resource")
@Singleton
public class DBConnectionSingleton {

    private DBConnectionFactory factory;

    public DBConnectionSingleton() {
        System.out.println("CREATING SINGLETON!!");

        String type = System.getenv("STUCCO_DB_TYPE");
        if (type == null) {
            throw (new NullPointerException("Missing environment variable STUCCO_DB_TYPE"));
        } 

        String config = System.getenv("STUCCO_DB_CONFIG");
        if (config == null) {
            throw (new NullPointerException("Missing environment variable STUCCO_DB_CONFIG"));
        }
        factory = DBConnectionFactory.getFactory(DBConnectionFactory.Type.valueOf(type));
        factory.setConfiguration(config);

        System.out.println("SINGLETON WAS CREATED!!");
    }

    private DBConnectionAlignment setDB() {
        DBConnectionAlignment db = factory.getDBConnectionTestInterface();
        db.open();

        return db;
    }

    public DBConnectionAlignment getDB() {
        return setDB();
    }

}

package gov.ornl.stucco.DBConnectionService;

import gov.pnnl.stucco.dbconnect.inmemory.InMemoryDBConnection;
import gov.pnnl.stucco.dbconnect.DBConnectionAlignment;
import gov.pnnl.stucco.dbconnect.DBConnectionFactory;
import gov.pnnl.stucco.dbconnect.elasticsearch.ElasticsearchDBConnection;
import gov.pnnl.stucco.dbconnect.elasticsearch.Connection;

import javax.inject.Singleton;
import javax.ws.rs.Path;

import java.net.UnknownHostException;

import java.io.FileNotFoundException;
 
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
    private DBConnectionAlignment db;

    private ElasticsearchDBConnection es;
    private Connection connection;

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
        db = factory.getDBConnectionTestInterface();

        config = System.getenv("SITU_DB_CONFIG");
        if (config == null) {
            throw (new NullPointerException("Missing environment variable SITU_DB_CONFIG"));
        }

        try {
            es = new ElasticsearchDBConnection(config); 
            connection = es.getConnection();
            connection.open();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        System.out.println("SINGLETON WAS CREATED!!");
    }

    public DBConnectionAlignment getDB() {
        return db;
    }

    public Connection getES() throws UnknownHostException {
        return connection;
    }

}

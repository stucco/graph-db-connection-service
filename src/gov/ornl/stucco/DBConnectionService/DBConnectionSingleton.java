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
 *       STUCCO_DB_TYPE= INMEMORY|ORIENTDB|TITAN|NEO4J
 */
@Path("/resource")
@Singleton
public class DBConnectionSingleton {

    private static DBConnectionFactory factory;
    private static DBConnectionAlignment db = null;

    public DBConnectionSingleton() {
        System.out.println("CREATING SINGLETON!!");

        String type = System.getenv("STUCCO_DB_TYPE");
        if (type == null) {
            throw (new NullPointerException("Missing environment variable STUCCO_DB_TYPE"));
        }

        factory = DBConnectionFactory.getFactory(DBConnectionFactory.Type.valueOf(type));

        String config = System.getenv("STUCCO_DB_CONFIG");
        if (config == null) {
            throw (new NullPointerException("Missing environment variable STUCCO_DB_CONFIG"));
        }
        factory.setConfiguration(config);

        db = factory.getDBConnectionTestInterface();
        db.open();

        //if using INMEMORY, load some existing state.
        if( type.equals("INMEMORY")){
            InMemoryDBConnection inMemoryDB = (InMemoryDBConnection)db;
            //inMemoryDB.load(true); //TODO this is for testing & demo, remove later.
            //inMemoryDB.loadStateFromJSON("/home/euf/stuccoDB/extractor_tests_output_small.json"); //TODO see above.
            //inMemoryDB.countVertexTypes();
            //inMemoryDB.loadStateFromJSON("/home/euf/stuccoDB/fireeye-pivy-report_demo.json"); //TODO see above.
            //inMemoryDB.countVertexTypes();
            //inMemoryDB.loadVertices("/home/euf/stuccoDB/testData/vertices.json"); //TODO see above.
            //inMemoryDB.loadEdges("/home/euf/stuccoDB/testData/edges.json"); //TODO see above.
            //inMemoryDB.loadStateFromJSON("/home/euf/stuccoDB/testData/asdf.json"); //TODO see above.
            inMemoryDB.loadState("/home/euf/stuccoDB/graph.json"); //TODO see above.
        }
        System.out.println("SINGLETON WAS CREATED!!");
    }

    public DBConnectionAlignment getDB() {
        return db;
    }

}

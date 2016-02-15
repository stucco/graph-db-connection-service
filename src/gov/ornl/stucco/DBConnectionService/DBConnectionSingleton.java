package gov.ornl.stucco.DBConnectionService;

import gov.ornl.stucco.DBClient.InMemoryDBConnection;

import javax.inject.Singleton;
import javax.ws.rs.Path;

/*
import javax.ws.rs.GET;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
*/

@Path("/resource")
@Singleton
public class DBConnectionSingleton {

	public static InMemoryDBConnection db = null;
	
	public DBConnectionSingleton() {
		System.out.println("CREATING SINGLETON!!");
		//this.db = DBConnectionSingleton.db;
		db = new InMemoryDBConnection();
		//db.load(true); //TODO this is for testing & demo, remove later.
		db.loadStateFromJSON("/tmp/stuccoDB/sample_data.json"); //TODO see above.
		System.out.println("SINGLETON WAS CREATED!!");
	}

	public InMemoryDBConnection getDB() {
		return db;
	}

}

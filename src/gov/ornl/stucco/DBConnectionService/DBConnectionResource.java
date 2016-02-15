package gov.ornl.stucco.DBConnectionService;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import org.glassfish.jersey.server.ResourceConfig;
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import gov.ornl.stucco.DBClient.Constraint;
import gov.ornl.stucco.DBClient.InMemoryDBConnection;
//import gov.ornl.stucco.DBClient.Constraint;
//import gov.ornl.stucco.DBClient.Constraint.Condition;
//import gov.ornl.stucco.DBClient.InvalidStateException;
import gov.ornl.stucco.DBClient.InvalidArgumentException;

@Path("/api")
public class DBConnectionResource extends ResourceConfig {
	
	@Inject
    private DBConnectionSingleton dbSingleton;
	
	InMemoryDBConnection db;
	
	public DBConnectionResource(){
		//TODO: use a singleton to hold the dbconnection object.
		//register(DBConnectionSingleton.class);
		//this.db = DBConnectionSingleton.db;
		//db = new InMemoryDBConnection();
		//db.load(true); //TODO this is for testing & demo, remove later.
		//db.loadStateFromJSON("/tmp/stuccoDB/sample_data.json"); //TODO see above.
		
		db = dbSingleton.db;
	}
	
	/*
	@Path("{todo}")
	public TodoResource getTodo(@PathParam("todo") String id) {
		return new TodoResource(uriInfo, request, id);
	}
	*/

	@GET
	@Path("inEdges/{vertID}")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
	public String getInEdges(@PathParam("vertID") String vertID, @QueryParam("q") String query) {
		Map<String, Integer> pageInfo = null;
		if(query != null){
			JSONObject queryObj = new JSONObject(query);
			pageInfo = findPageInfo( queryObj );
		}else{
			pageInfo = findPageInfo( new JSONObject() );//to get defaults
		}
		
		JSONObject ret = new JSONObject();
		try {
			JSONArray results = getInEdgesResults(vertID);
			//TODO: it would be faster to pass the start/end to getInEdgesResults() above.
			int page = pageInfo.get("page");
			int pageSize = pageInfo.get("pageSize");
			JSONArray selectedResults;
			if(pageSize > 0){
				selectedResults = new JSONArray();
				int start = page * pageSize;
				int end = (page + 1) * pageSize;
				for(int i = start; i<results.length() && i<end; i++){
					selectedResults.put( results.get(i) );
				}
			}else{
				selectedResults = results;
			}
			
			ret.put("results", selectedResults); //TODO
			ret.put("count",results.length());//TODO total set size, or returned subset size?
			ret.put("success",true);//TODO
			ret.put("version", ""); //TODO
			ret.put("queryTime", ""); //TODO
		} catch (InvalidArgumentException e) {
			e.printStackTrace();//TODO proper logging
			ret.put("Error:", "Illegal State Exception");
		}
		return ret.toString();
	}
	
	private JSONArray getInEdgesResults(String vertID) throws InvalidArgumentException {
		List<Map<String, Object>> foundEdges = db.getInEdges(vertID);
		System.out.println("inEdges found " + foundEdges.size() + " edges.");
		JSONArray results = new JSONArray();
		for(Map<String,Object> edge : foundEdges){
			System.out.println("found edge with keys: " + edge.keySet());
			System.out.println("  outVertID: " + edge.get("outVertID"));
			System.out.println("  inVertID: " + edge.get("inVertID"));
			System.out.println("  relation: " + edge.get("relation"));
			JSONArray resultItem = new JSONArray();
			JSONObject inV = getVertexResults((String)edge.get("inVertID"));
			JSONObject outV = getVertexResults((String)edge.get("outVertID")); 
			JSONObject e = new JSONObject();
			e.put("_inV", (String)edge.get("inVertID"));
			e.put("inVType", (String)inV.optString("vertexType"));
			e.put("_outV", (String)edge.get("outVertID"));
			e.put("outVType", (String)outV.optString("vertexType"));
			e.put("label", (String)edge.get("relation"));
			e.put("description", (String)edge.get("outVertID") + " : " 
					+ (String)edge.get("relation")  + " : " + (String)edge.get("inVertID"));
			resultItem.put(inV);
			resultItem.put(e);
			resultItem.put(outV);
			results.put(resultItem);
		}
		return results;
	}
	
	@GET
	@Path("outEdges/{vertID}")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
	public String getOutEdges(@PathParam("vertID") String vertID, @QueryParam("q") String query) {
		Map<String, Integer> pageInfo = null;
		if(query != null){
			JSONObject queryObj = new JSONObject(query);
			pageInfo = findPageInfo( queryObj );
		}else{
			pageInfo = findPageInfo( new JSONObject() );//to get defaults
		}
		
		JSONObject ret = new JSONObject();
		try {
			JSONArray results = getOutEdgesResults(vertID);
			//TODO: it would be faster to pass the start/end to getInEdgesResults() above.
			int page = pageInfo.get("page");
			int pageSize = pageInfo.get("pageSize");
			JSONArray selectedResults;
			if(pageSize > 0){
				selectedResults = new JSONArray();
				int start = page * pageSize;
				int end = (page + 1) * pageSize;
				for(int i = start; i<results.length() && i<end; i++){
					selectedResults.put( results.get(i) );
				}
			}else{
				selectedResults = results;
			}
			
			ret.put("results", selectedResults); //TODO
			ret.put("count",results.length());//TODO total set size, or returned subset size?
			ret.put("success",true);//TODO
			ret.put("version", ""); //TODO
			ret.put("queryTime", ""); //TODO
		} catch (InvalidArgumentException e) {
			e.printStackTrace();//TODO proper logging
			ret.put("Error:", "Illegal State Exception");
		}
		return ret.toString();
	}
	
	private JSONArray getOutEdgesResults(String vertID) throws InvalidArgumentException {
		JSONArray results = new JSONArray();
		List<Map<String, Object>> foundEdges = db.getOutEdges(vertID);
		for(Map<String,Object> edge : foundEdges){
			System.out.println("found edge with keys: " + edge.keySet());
			System.out.println("  outVertID: " + edge.get("outVertID"));
			System.out.println("  inVertID: " + edge.get("inVertID"));
			System.out.println("  relation: " + edge.get("relation"));
			JSONArray resultItem = new JSONArray();
			JSONObject inV = getVertexResults((String)edge.get("inVertID"));
			JSONObject outV = getVertexResults((String)edge.get("outVertID")); 
			JSONObject e = new JSONObject();
			e.put("_inV", (String)edge.get("inVertID"));
			e.put("inVType", (String)inV.optString("vertexType"));
			e.put("_outV", (String)edge.get("outVertID"));
			e.put("outVType", (String)outV.optString("vertexType"));
			e.put("label", (String)edge.get("relation"));
			e.put("description", (String)edge.get("outVertID") + " : " 
					+ (String)edge.get("relation")  + " : " + (String)edge.get("inVertID"));
			resultItem.put(outV);
			resultItem.put(e);
			resultItem.put(inV);
			results.put(resultItem);
		}
		return results;
	}
	
	@GET
	@Path("vertex/{vertID}")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
	public String getVertex(@PathParam("vertID") String vertID) {
		JSONObject ret = new JSONObject();
		JSONObject results = getVertexResults(vertID);
		ret.put("results", results);
		ret.put("count",1);//TODO verify
		ret.put("success",true);//TODO
		ret.put("version", ""); //TODO
		ret.put("queryTime", ""); //TODO
		return ret.toString();
	}
	
	private JSONObject getVertexResults(String vertID) {
		JSONObject results = new JSONObject();
		Map<String,Object> vert = db.getVertByID(vertID);
		if(vert != null){
			for(String k : vert.keySet()){
				results.put(k, vert.get(k));
			}
			results.put("_id", vertID);
		}
		return results;
	}
	
	@GET
	@Path("search")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
	public String search(@QueryParam("q") String query) {
		System.out.println("search() query is: " + query);
		JSONObject queryObj = new JSONObject(query);
		
		Map<String, Integer> pageInfo = findPageInfo( queryObj );
		queryObj.remove("page");
		queryObj.remove("pageSize");
		
		JSONObject ret = new JSONObject();
		JSONArray results = searchResults( queryObj);
		System.out.println("results are: " + results);
		
		int page = pageInfo.get("page");
		int pageSize = pageInfo.get("pageSize");
		JSONArray selectedResults;
		if(pageSize > 0){
			selectedResults = new JSONArray();
			int start = page * pageSize;
			int end = (page + 1) * pageSize;
			for(int i = start; i<results.length() && i<end; i++){
				selectedResults.put( results.get(i) );
			}
		}else{
			selectedResults = results;
		}
		
		ret.put("results", selectedResults); //TODO
		ret.put("count",results.length());//TODO total set size, or returned subset size?
		ret.put("success",true);//TODO
		ret.put("version", ""); //TODO
		ret.put("queryTime", ""); //TODO
		return ret.toString();
	}
	
	private JSONArray searchResults(JSONObject queryObj) {
		List<Constraint> constraints = new LinkedList<Constraint>();
		Constraint c;
		for(Object key : queryObj.keySet()){
			//TODO: proper handling for non-strings, like {"description":["foo","bar"]}, which ui can generate
			String val = queryObj.optString((String) key);
			if(val.equals("")){
				System.out.println("cannot handle value for key of: " + key);
			}else{
				System.out.println("query includes key of [" + key + ", " + val + "]");
				
				//TODO: check and correct case of keys?  eg. change "VertexType" to "vertexType"?  (or handle in ui code.)
				//check fields which need special handling, eg. description
				//TODO: any other fields?
				if(key.equals("description")){
					c = new Constraint((String)key, Constraint.Condition.in, val);
				}else{
					c = new Constraint((String)key, Constraint.Condition.eq, val);
				}
				constraints.add(c);
			}
		}
		List<String> foundIDs = db.getVertIDsByConstraints(constraints);
		JSONArray results = new JSONArray();
		for(String id : foundIDs){
			JSONObject foundVert = getVertexResults(id);
			results.put(foundVert);
		}
		return results;
	}
	
	@GET
	@Path("count/vertices")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
	public String countVertices() {
		JSONObject ret = new JSONObject();
		int count = db.getVertCount();
		ret.put("count", count);
		ret.put("success",true);//TODO
		ret.put("version", ""); //TODO
		ret.put("queryTime", ""); //TODO
		return ret.toString();
	}
	
	@GET
	@Path("count/edges")
	@Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
	public String countEdges() {
		JSONObject ret = new JSONObject();
		int count = db.getEdgeCount();
		ret.put("count", count);
		ret.put("success",true);//TODO
		ret.put("version", ""); //TODO
		ret.put("queryTime", ""); //TODO
		return ret.toString();
	}
	
	private Map<String, Integer> findPageInfo( JSONObject queryObj ){
		Map<String, Integer> info = new HashMap<String, Integer>();

		int page = 0;
		int pageSize = 0;
		
		//handles page vals as str or int.
		String pageString = queryObj.optString("page");
		if(pageString == null || pageString.equals("")){
			page = queryObj.optInt("page");
		}else{
			page = Integer.parseInt(pageString);
		}
		
		String pageSizeString = queryObj.optString("pageSize");
		if(pageSizeString == null || pageSizeString.equals("")){
			pageSize = queryObj.optInt("pageSize");
		}else{
			pageSize = Integer.parseInt(pageSizeString);
		}
		
		info.put("page", page);
		info.put("pageSize", pageSize);	
		
		System.out.println("page info is: " + info);
		
		return info;
	}

} 
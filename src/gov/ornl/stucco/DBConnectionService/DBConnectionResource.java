package gov.ornl.stucco.DBConnectionService;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import java.net.UnknownHostException;

import javax.inject.Inject; 
import javax.ws.rs.GET;
import javax.ws.rs.Path; 
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes; 
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
 
import org.glassfish.jersey.server.ResourceConfig; 
import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger; 
import org.slf4j.LoggerFactory;

import gov.pnnl.stucco.dbconnect.Condition;
import gov.pnnl.stucco.dbconnect.DBConnectionAlignment;
import gov.pnnl.stucco.dbconnect.DBConstraint;

import gov.pnnl.stucco.dbconnect.elasticsearch.Connection;
import gov.pnnl.stucco.dbconnect.elasticsearch.ElasticsearchDBConstraint;
 
@Path("/api")
public class DBConnectionResource extends ResourceConfig {

    private final int CONNECTION_RETRIES = 3;
    DBConnectionSingleton dbSingleton;

    public DBConnectionResource(DBConnectionSingleton dbSingleton) {
        this.dbSingleton = dbSingleton;
    }

    @GET
    @Path("inEdges/vertexType={vertType}&name={vertName}&id={vertID}")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    public String getInEdges(@PathParam("vertType") String vertType, @PathParam("vertName") String vertName, @PathParam("vertID") String vertID, @QueryParam("q") String query) {
        Map<String, Integer> pageInfo = null;
        if (query != null) { 
            JSONObject queryObj = new JSONObject(query);
            pageInfo = findPageInfo(queryObj);
        } else {
            pageInfo = findPageInfo(new JSONObject()); //to get defaults
        }

        JSONObject ret = new JSONObject();
        try {
            JSONArray results = getInEdgesResult(vertID, vertName, vertType, pageInfo);
            
            ret.put("results", results); //TODO
            ret.put("count", results.length());//TODO total set size, or returned subset size?
            ret.put("success", true);//TODO
            ret.put("version", ""); //TODO
            ret.put("queryTime", ""); //TODO
        } catch (IllegalArgumentException e) {
            e.printStackTrace();//TODO proper logging
            ret.put("Error:", "Illegal State Exception");
        }

        return ret.toString();
    }

    private JSONArray getInEdgesResult(String vertID, String vertName, String vertType, Map<String, Integer> pageInfo) {
        JSONArray edges = getInEdgesFromStucco(vertID, pageInfo);
        if (edges.length() < pageInfo.get("pageSize")) {
            DBConnectionAlignment db = dbSingleton.getDB();
            long count = db.getInEdgeCount(vertID);
            pageInfo.put("page", pageInfo.get("page") - (int) Math.floor(count / pageInfo.get("pageSize")));
            pageInfo.put("pageSize", pageInfo.get("pageSize") - edges.length());
            if (edges.length() == 0) {
                edges = getInEdgesFromSitu(vertName, vertType, pageInfo);
            } else {
                JSONArray additionalEdges = getInEdgesFromSitu(vertName, vertType, pageInfo);
                for (int i = 0; i < additionalEdges.length(); i++) {
                    edges.put(additionalEdges.get(i));
                }
            }
        }

        return edges;
    }

    private JSONArray getInEdgesFromStucco(String vertID, Map<String, Integer> pageInfo) {
        JSONArray edges = new JSONArray();
        int page = pageInfo.get("page");
        int pageSize = pageInfo.get("pageSize");
        System.out.println("vertID: " +  vertID + " pageSize: " + pageSize + " page: " + page);
        DBConnectionAlignment db = dbSingleton.getDB();
        List<Map<String, Object>> foundEdges = db.getInEdgesPage(vertID, page * pageSize, pageSize);

        for (Map<String,Object> edge : foundEdges) {
            JSONObject outV = getVertexStucco((String)edge.get("outVertID"));
            edges.put(outV);
        }

        return edges;
    }

    private JSONArray getInEdgesFromSitu(String vertName, String vertType, Map<String, Integer> pageInfo) {
        JSONArray edges = new JSONArray();
        try {
            Connection con = dbSingleton.getES();
            edges = con.getInEdgesPage(vertName, vertType, pageInfo.get("page"), pageInfo.get("pageSize"));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        
        return edges;
    }

    @GET
    @Path("outEdges/vertexType={vertType}&name={vertName}&id={vertID}")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    public String getOutEdges(@PathParam("vertType") String vertType, @PathParam("vertName") String vertName, @PathParam("vertID") String vertID, @QueryParam("q") String query) {
        Map<String, Integer> pageInfo = null;
        if (query != null) { 
            JSONObject queryObj = new JSONObject(query);
            pageInfo = findPageInfo(queryObj);
            System.out.println("query is null, so page info is: " + pageInfo);
        } else {
            pageInfo = findPageInfo(new JSONObject()); //to get defaults
            System.out.println("query is NOT null, so page info is: " + pageInfo);
        }

        JSONObject ret = new JSONObject();
        try {
            JSONArray results = getOutEdgesResult(vertID, vertName, vertType, pageInfo);
            
            ret.put("results", results); //TODO
            ret.put("count", results.length());//TODO total set size, or returned subset size?
            ret.put("success", true);//TODO
            ret.put("version", ""); //TODO
            ret.put("queryTime", ""); //TODO
        } catch (IllegalArgumentException e) {
            e.printStackTrace();//TODO proper logging
            ret.put("Error:", "Illegal State Exception");
        }

        return ret.toString();
    }

    private JSONArray getOutEdgesResult(String vertID, String vertName, String vertType, Map<String, Integer> pageInfo) {
        JSONArray edges = getOutEdgesFromStucco(vertID, pageInfo);
        if (edges.length() < pageInfo.get("pageSize")) {
            DBConnectionAlignment db = dbSingleton.getDB();
            long count = db.getOutEdgeCount(vertID);
            pageInfo.put("page", pageInfo.get("page") - (int) Math.floor(count / pageInfo.get("pageSize")));
            pageInfo.put("pageSize", pageInfo.get("pageSize") - edges.length());
            if (edges.length() == 0) {
                edges = getOutEdgesFromSitu(vertName, vertType, pageInfo);
            } else {
                JSONArray additionalEdges = getOutEdgesFromSitu(vertName, vertType, pageInfo);
                for (int i = 0; i < additionalEdges.length(); i++) {
                    edges.put(additionalEdges.get(i));
                }
            }
        }

        return edges;
    }

    private JSONArray getOutEdgesFromStucco(String vertID, Map<String, Integer> pageInfo) {
        JSONArray edges = new JSONArray();
        int page = pageInfo.get("page");
        int pageSize = pageInfo.get("pageSize");
        System.out.println("vertID: " +  vertID + " pageSize: " + pageSize + " page: " + page);
        DBConnectionAlignment db = dbSingleton.getDB();
        List<Map<String, Object>> foundEdges = db.getOutEdgesPage(vertID, page * pageSize, pageSize);
        System.out.println("foundEdges size: " + foundEdges.size());

        for (Map<String,Object> edge : foundEdges) {
            JSONObject inV = getVertexStucco((String)edge.get("inVertID"));
            edges.put(inV);
        }

        return edges;
    }

    private JSONArray getOutEdgesFromSitu(String vertName, String vertType, Map<String, Integer> pageInfo) {
        JSONArray edges = new JSONArray();
        try {
            Connection con = dbSingleton.getES();
            edges = con.getOutEdgesPage(vertName, vertType, pageInfo.get("page"), pageInfo.get("pageSize"));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        
        return edges;
    }

    @GET
    @Path("vertex/vertexType={vertType}&name={vertName}&id={vertID}")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    public String getVertex(@PathParam("vertType") String vertType, @PathParam("vertName") String vertName, @PathParam("vertID") String vertID) {
        System.out.println("vertName = " + vertName);
        System.out.println("vertID = " + vertID);
        System.out.println("vertType = " + vertType);

        JSONObject result = getVertexStucco(vertID);
        if (result.length() == 0) {
            result = getVertexSitu(vertType, vertName);
        }

        JSONObject ret = new JSONObject();
        ret.put("results", result);
        ret.put("count",1); //TODO verify 
        ret.put("success",true); //TODO
        ret.put("version", ""); //TODO
        ret.put("queryTime", ""); //TODO
        System.out.println("Exiting with vertex results!!! ");

        return ret.toString();
    }

    private JSONObject getVertexResults(String vertID) {
        DBConnectionAlignment db = dbSingleton.getDB();
        Map<String,Object> vert = db.getVertByID(vertID);
        JSONObject results = new JSONObject();

        if (vert != null) {
            for (String k : vert.keySet()) {
                results.put(k, vert.get(k));
            }
            results.put("_id", vertID);
        }

        return results;
    }

    private JSONObject getVertexStucco(String vertID) {
        DBConnectionAlignment db = dbSingleton.getDB();
        Map<String,Object> vert = db.getVertByID(vertID);
        JSONObject results = new JSONObject();

        if (vert != null) {
            for (String k : vert.keySet()) {
                results.put(k, vert.get(k));
            }
            results.put("_id", vertID);
        }

        return results;
    }

    private JSONObject getVertexSitu(String vertexType, String vertName) {
        JSONObject result = new JSONObject();

        try {
            Connection con = dbSingleton.getES();
            result = con.getVertByName(vertName);
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        return result;
    }

    /**
     * search query where source is Stucco or Situ
     */
    @GET
    @Path("search")
    @Consumes({ MediaType.APPLICATION_JSON })
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    public String search(@QueryParam("q") String query) {
        System.out.println("search() query is: " + query);
        
        JSONObject queryObj = new JSONObject(query);
        Map<String, Integer> pageInfo = findPageInfo(queryObj);
        queryObj.remove("page");
        queryObj.remove("pageSize");

        JSONObject ret = new JSONObject();
        JSONArray results = searchResults(queryObj, pageInfo, ret);
        ret.put("results", results); //TODO
        ret.put("count",results.length()); //TODO total set size, or returned subset size?
        ret.put("success",true); //TODO
        ret.put("version", ""); //TODO
        ret.put("queryTime", ""); //TODO

        System.out.println("search reslts: " + results.length());

        return ret.toString();
    }

    private JSONArray searchResults(JSONObject queryObj, Map<String, Integer> pageInfo, JSONObject ret) {
        JSONArray results = queryStucco(queryObj, pageInfo);
        if (results.length() < pageInfo.get("pageSize")) {
            DBConnectionAlignment db = dbSingleton.getDB();
            List<DBConstraint> constraints = getStuccoConstraints(queryObj, db);
            long count = db.getVertCountByConstraints(constraints);   
            pageInfo.put("page", pageInfo.get("page") - (int) Math.floor(count / pageInfo.get("pageSize")));
            pageInfo.put("pageSize", pageInfo.get("pageSize") - results.length());
            if (results.length() == 0) {
                results = querySitu(queryObj, pageInfo);
            } else {
                JSONArray additionalResults = querySitu(queryObj, pageInfo);
                if (additionalResults != null) {
                    for (int i = 0; i < additionalResults.length(); i++) {
                        results.put(additionalResults.get(i));
                    }
                }
            }
        }

        return results;
    }

    private JSONArray queryStucco(JSONObject queryObj, Map<String, Integer> pageInfo) {
        DBConnectionAlignment db = dbSingleton.getDB();
        List<DBConstraint> constraints = getStuccoConstraints(queryObj, db);
        List<String> foundIDs = db.getVertIDsByConstraints(constraints, pageInfo.get("page") * pageInfo.get("pageSize"), pageInfo.get("pageSize"));   
        JSONArray results = new JSONArray(); 
        for (String id : foundIDs) {
            JSONObject foundVert = getVertexStucco(id);
            results.put(foundVert);
        }

        return results;
    }

    private List<DBConstraint> getStuccoConstraints(JSONObject queryObj, DBConnectionAlignment db) {
        List<DBConstraint> constraints = new LinkedList<DBConstraint>();
        DBConstraint c;
        for (Object key : queryObj.keySet()) {
            //TODO: proper handling for non-strings, like {"description":["foo","bar"]}, which ui can generate
            String val = queryObj.optString((String) key);
            if (val.equals("")) {
                System.out.println("cannot handle value for key of: " + key);
            } else {
                System.out.println("query includes key of [" + key + ", " + val + "]");

                //TODO: check and correct case of keys?  eg. change "VertexType" to "vertexType"?  (or handle in ui code.)
                //check fields which need special handling, eg. description
                //TODO: any other fields?
                if (key.equals("description") || key.equals("sourceDocument")) {
                    c = db.getConstraint((String)key, Condition.contains, val);
                } else {
                    c = db.getConstraint((String)key, Condition.eq, val);
                }
                constraints.add(c);
            }
        }

        return constraints;
    }

    private JSONArray querySitu(JSONObject queryObj, Map<String, Integer> pageInfo) {
        JSONArray results = null;
        try {
            Connection con = dbSingleton.getES();
            List<DBConstraint> constraints = getSituConstraints(queryObj, con);
            results = con.getVertsByConstraints((List<DBConstraint>) constraints, pageInfo.get("page"), pageInfo.get("pageSize"));
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        return results;
    }

    private List<DBConstraint> getSituConstraints(JSONObject queryObj, Connection con) {
        List<DBConstraint> constraints = new LinkedList<DBConstraint>();

            for (Object key : queryObj.keySet()) {
            //TODO: proper handling for non-strings, like {"description":["foo","bar"]}, which ui can generate
            String val = queryObj.optString((String) key);
            if (val.equals("")) {
                System.out.println("cannot handle value for key of: " + key);
            } else {
                System.out.println("query includes key of [" + key + ", " + val + "]");
                constraints.add(new ElasticsearchDBConstraint(key.toString(), Condition.eq, val));
            }
        }

        return constraints;
    }

    @GET
    @Path("count/vertices")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    public String countVerticesByTimestamp(@QueryParam("q") String query) {
        JSONArray result = new JSONArray();
        long count = 0L;
        DBConnectionAlignment db = dbSingleton.getDB();
        if (query != null) {
            List<DBConstraint> constraints = new LinkedList<DBConstraint>();
            constraints.add(null);
            JSONObject queryObj = new JSONObject(query);
            int days = (int) queryObj.opt("days");
            for (int i = 0; i < days; i++) {
                constraints.set(0, db.getConstraint("date(timestamp)", Condition.eq, "(current_date - " + i + ")"));
                count = db.getVertCountByConstraints(constraints);
                result.put(count);
            }
        } else {
            count = db.getVertCount();
        }

        JSONObject ret = new JSONObject();
        ret.put("count", result);
        ret.put("success",true);//TODO
        ret.put("version", ""); //TODO
        ret.put("queryTime", ""); //TODO

        return ret.toString();
    }

    @GET
    @Path("count/edges")
    @Produces({ MediaType.APPLICATION_JSON, MediaType.TEXT_PLAIN })
    public String countEdges(@QueryParam("q") String query) {
        DBConnectionAlignment db = dbSingleton.getDB();
        db.open();

        long count;
        //TODO: can't query & count edges currently.  Is that even useful to have?
        count = db.getEdgeCount();
        JSONObject ret = new JSONObject();
        ret.put("count", count);
        ret.put("success",true); //TODO
        ret.put("version", ""); //TODO
        ret.put("queryTime", ""); //TODO

        db.close();

        return ret.toString();
    }

    private Map<String, Integer> findPageInfo( JSONObject queryObj ){
        Map<String, Integer> info = new HashMap<String, Integer>();

        int page = 0;
        int pageSize = 0;

        //handles page vals as str or int.
        String pageString = queryObj.optString("page");
        if (pageString == null || pageString.equals("")) {
            page = queryObj.optInt("page");
        } else {
            page = Integer.parseInt(pageString); 
        }

        String pageSizeString = queryObj.optString("pageSize");
        if (pageSizeString == null || pageSizeString.equals("")) {
            pageSize = queryObj.optInt("pageSize");
        } else {
            pageSize = Integer.parseInt(pageSizeString);
        }

        info.put("page", page);
        info.put("pageSize", pageSize);   

        return info;
    }

    private JSONArray mergeResults(JSONArray results1, JSONArray results2) {
        for (int i = 0; i < results1.length(); i++) {
            JSONObject vert1 = results1.getJSONObject(i);
            String name1 = vert1.getString("name");
            String vertexType1 = vert1.getString("vertexType");
            String observableType1 = vert1.optString("observableType");
            
            for (int j = 0; j < results2.length(); j++) {
                JSONObject vert2 = results2.getJSONObject(j);
                String name2 = vert2.getString("name");
                if (name1.equals(name2)) {
                    String observableType2 = vert2.getString("observableType");
                    String vertexType2 = vert2.getString("vertexType");
                    if (observableType1.equals(observableType2) && vertexType1.equals(vertexType2)) {
                        mergeVerts(vert1, vert2);
                        results2.remove(j);
                        break;
                    }
                }
            }
        }

        return results1;
    }

    private Map<String,Object> mergeVerts(Map<String,Object> vert1, Map<String,Object> vert2) {
        for (String key : vert2.keySet()) {
            Object value = vert2.get(key);
            if (!vert1.containsKey(key)) {
                vert1.put(key, value);
            } else if (value instanceof java.util.Collection) {
                Set<Object> set = (Set<Object>) vert1.get(key);
                set.addAll((HashSet<Object>) vert2.get(key));
                vert1.put(key, (Object) set);
            }
        }

        return vert1;
    }

    private JSONObject mergeVerts(JSONObject vert1, JSONObject vert2) {
        for (String key : (Set<String>) vert2.keySet()) {
            Object value = vert2.get(key);
            if (!vert1.has(key)) {
                vert1.put(key, value);
            } else if (value instanceof java.util.Collection) {
                Set<Object> set = (Set<Object>) vert1.get(key);
                set.addAll((HashSet<Object>) vert2.get(key));
                vert1.put(key, (Object) set);
            }
        }

        return vert1;
    }
} 

package com.enremmeta.otter.jersey;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.enremmeta.otter.CdhConnection;
import com.enremmeta.otter.Impala;
import com.enremmeta.otter.OfficeDb;
import com.enremmeta.otter.OtterException;
import com.enremmeta.otter.entity.Dataset;

@Path("/dataset")
public class DatasetSvc {
	@PUT
	@Path("/create")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Map<String, Integer> create(Dataset ds) throws Exception {
		OfficeDb db = OfficeDb.getInstance();
		CdhConnection cdhc = CdhConnection.getInstance();
		Impala imp = Impala.getInstance();

		// 1. Copy file to cdh.upload_path
		// 2. Copy to HDFS
		// 3 Create external table at /otter/...
		// 4.* Create internal table
		// 5.* Copy with partitioning
		Map<String, Integer> retval = new HashMap<String, Integer>();
		try {
			cdhc.addDataset(ds);
			imp.addDataset(ds);
			int id = db.addDataset(ds);
			retval.put("id", id);
		} catch (SQLException sqle) {
			throw new OtterException(sqle);
		} catch (Exception e) {
			throw new OtterException(e);
		}
		return retval;
	}

	@GET
	@Path("{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public String get(@PathParam("id") String id) {
		return "Got it!";
	}

	@GET
	@Path("/sample/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public String sample(@PathParam("id") String id) {
		return "Got it!";
	}

	@PUT
	@Path("/load/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public String load(@PathParam("id") String id) {
		return "";
	}

	@DELETE
	@Path("{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public String delete(@PathParam("id") String id) {
		return "Got it!";
	}
}

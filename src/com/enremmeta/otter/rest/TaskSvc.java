package com.enremmeta.otter.rest;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.enremmeta.otter.Impala;
import com.enremmeta.otter.OfficeDb;
import com.enremmeta.otter.OtterException;
import com.enremmeta.otter.entity.Task;

@Path("/task")
public class TaskSvc {

	private OfficeDb odb;
	private Impala imp;
	
	@SuppressWarnings({ "rawtypes", "unchecked" })
	@GET
	@Path("/run/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Map runTask(@PathParam("id") int id) throws OtterException {
		Task task = odb.getTask(id);
		Map retval = new HashMap();

		// Do filter, but tihs is confusing.
		// Bit of a hack for now
		List<String> sqls = imp.buildPrepSql(task);
		// retval.put("sql", sql);
		//
		// Map rs = impala.query(sql);
		// retval.put("result_set", rs);
		return retval;

	}
}

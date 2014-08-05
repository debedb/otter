package com.enremmeta.otter.jersey;

import java.sql.SQLException;
import java.util.HashMap;
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

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@GET
	@Path("/run/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Map runTask(@PathParam("id") int id) throws OtterException {
		try {
			OfficeDb db = OfficeDb.getInstance();
			Task task = db.getTask(id);

			// Do filter, but tihs is confusing.
			// Bit of a hack for now
			Impala impala = Impala.getInstance();
			String sql = impala.buildSql(task);
			Map retval = new HashMap();
			retval.put("sql", sql);
			Map rs = impala.query(sql);
			retval.put("result_set", rs);
			return retval;
		} catch (SQLException sqle) {
			throw new OtterException(sqle);
		}
	}

}

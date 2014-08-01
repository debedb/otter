package com.enremmeta.otter.jersey;

import java.sql.SQLException;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.enremmeta.otter.OfficeDb;
import com.enremmeta.otter.OtterException;
import com.enremmeta.otter.entity.Task;

@Path("/task")
public class TaskSvc {

	@GET
	@Path("/run/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public void getMeta(@PathParam("id") int id) throws OtterException {
		OfficeDb db = OfficeDb.getInstance();
		Task ds = db.getTask(id);

	}

}

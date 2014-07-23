package com.enremmeta.otter.jersey;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;

import com.enremmeta.otter.OtterException;

@Path("/jobs")
public class JobsSvc {

	@GET
	@Path("/status/{id}")
	@Consumes(MediaType.APPLICATION_JSON)
	public Map jobStatus(@PathParam("id") String id) throws OtterException {
		Map map = new HashMap<>();
		
		return map;
	}
}

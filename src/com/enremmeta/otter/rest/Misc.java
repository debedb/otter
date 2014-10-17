package com.enremmeta.otter.rest;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.enremmeta.otter.Constants;
import com.enremmeta.otter.OtterException;
import com.enremmeta.otter.WebServer;
import com.enremmeta.otter.entity.messages.QueryMessage;

@Path("/")
public class Misc {

	public Misc() {
		super();

	}

	@GET
	@Path("")
	@Produces(MediaType.APPLICATION_JSON)
	public Map info() {
		Map map = new HashMap();
		map.put("version", Constants.VERSION);
		return map;
	}

	@POST
	@Path("query")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Map query(QueryMessage q) throws OtterException {
		try {
			Map map = new HashMap();
			map = WebServer.getWorkhorse().query(q);
			return map;
		} catch (Exception e) {
			throw new OtterException(e);
		}
	}

}

package com.enremmeta.otter.jersey;

import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

import com.enremmeta.otter.CdhConnection;
import com.enremmeta.otter.Impala;
import com.enremmeta.otter.OfficeDb;
import com.enremmeta.otter.OtterException;

@Path("/test")
public class Test {

	@POST
	@Path("/cleanup")
	@Produces(MediaType.APPLICATION_JSON)
	public Map cleanup() throws OtterException {
		OfficeDb db = OfficeDb.getInstance();
		CdhConnection cdhc = CdhConnection.getInstance();
		Impala imp = Impala.getInstance();
		Map<String, String> map = new HashMap<String, String>();

		int upd = db.testCleanup();
		imp.testCleanup();
		cdhc.testCleanup();
		map.put("deleted_from_meta_db", "" + upd);
		
		return map;
	}

}

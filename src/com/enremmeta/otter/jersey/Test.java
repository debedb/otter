package com.enremmeta.otter.jersey;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

	private OfficeDb db;
	private CdhConnection cdhc;
	private Impala imp;

	@POST
	@Path("/cleanup")
	@Produces(MediaType.APPLICATION_JSON)
	public Map cleanup() throws OtterException {
		Map map = new HashMap();

		int upd = db.testCleanup();

		List<String> errs = imp.testCleanup();
		String err2 = cdhc.testCleanup();
		if (err2 != null) {
			if (errs == null) {
				errs = new ArrayList<String>();
			}
			errs.add(err2);
		}
		if (errs != null) {
			map.put("errors", errs);
		}

		map.put("deleted_from_meta_db", "" + upd);
		return map;
	}

}

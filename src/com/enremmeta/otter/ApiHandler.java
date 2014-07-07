package com.enremmeta.otter;

import java.io.File;
import java.util.Map;

import com.enremmeta.otter.entity.Dataset;


/**
 * General structure being <method>_<noun>_<verb>
 * 
 * @author grisha
 */
public class ApiHandler {
	
	public void put_dataset_create(Object arg) throws Exception {
		
	}

	public void post_dataset_loaddata(Object arg) throws Exception {
		Map map = (Map)arg;
		int datasetId = (int)map.get("id");
		Map source = (Map)map.get("source");
		String path = (String)source.get("path");
		// TODO
		
		OfficeDb db = OfficeDb.getInstance();
		Dataset ds = db.getDataset(datasetId);
		if (ds == null) {
			throw new RuntimeException("Dataset " + datasetId + " not found.");
		}
		
		CdhConnection cdhc = CdhConnection.getInstance();
		
//		cdhc.upload(path);
		
		String fname = new File(path).getName();
		cdhc.loadData(ds, fname);
		Impala impala = Impala.getInstance();
		impala.getCount(ds.getName()); 
	}
}

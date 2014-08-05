package com.enremmeta.otter.jersey;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.enremmeta.otter.CdhConnection;
import com.enremmeta.otter.Config;
import com.enremmeta.otter.Constants;
import com.enremmeta.otter.Impala;
import com.enremmeta.otter.Logger;
import com.enremmeta.otter.OfficeDb;
import com.enremmeta.otter.OtterException;
import com.enremmeta.otter.entity.Dataset;
import com.enremmeta.otter.entity.LoadSource;

@Path("/dataset")
public class DatasetSvc {
	@PUT
	@Path("/create")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Map<String, Integer> create(Dataset ds) throws OtterException {
		if (!Config.getInstance().getBooleanProperty("otter.manage_metadata")) {
			throw new OtterException(
					"Either set otter.manage_metadata to true, or use "
							+ "GET /dataset/create/{id} method instead to "
							+ "pull metadata from the DB");
		}
		Logger.log("Entered /dataset/create, thread " + Thread.currentThread());
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
	@Path("/create/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Map<String, Integer> create(@PathParam("id") int id)
			throws OtterException {
		Logger.log("Entered GET /dataset/create/" + id + ", thread "
				+ Thread.currentThread());
		OfficeDb db = OfficeDb.getInstance();
		CdhConnection cdhc = CdhConnection.getInstance();
		Impala imp = Impala.getInstance();

		Map<String, Integer> retval = new HashMap<String, Integer>();
		try {
			OfficeDb odb = OfficeDb.getInstance();
			Dataset ds = odb.getDataset(id);
			cdhc.addDataset(ds);
			imp.addDataset(ds);
		} catch (Exception sqle) {
			throw new OtterException(sqle);
		}
		return retval;
	}

	@GET
	@Path("/data/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public List getSample(@PathParam("id") int id, @QueryParam("rows") int rows)
			throws OtterException {
		@SuppressWarnings("rawtypes")
		List list = new ArrayList();
		try {
			Impala imp = Impala.getInstance();
			OfficeDb db = OfficeDb.getInstance();
			Dataset ds = db.getDataset(id);
			list = imp.getSample(ds.getName(), rows);
		} catch (SQLException sqle) {
			throw new OtterException(sqle);
		}
		return list;
	}

	@GET
	@Path("/meta/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Dataset getMeta(@PathParam("id") String id) throws OtterException {
		OfficeDb db = OfficeDb.getInstance();
		try {
			Dataset ds = db.getDataset(Integer.parseInt(id));
			return ds;
		} catch (SQLException sqle) {
			throw new OtterException(sqle);
		}
	}

	@PUT
	@Path("/load/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Map uploadDataset(@PathParam("id") String id,
			List<LoadSource> sources) throws OtterException {
		OfficeDb db = OfficeDb.getInstance();
		Impala imp = Impala.getInstance();
		CdhConnection cdhc = CdhConnection.getInstance();

		Map map = new HashMap();
		Config config = Config.getInstance();
		try {
			Dataset ds = db.getDataset(Integer.parseInt(id));
			String dsName = ds.getName();
			long rowsBefore = imp.getCount(Config.getInstance()
					.getImpalaDbName() + "." + dsName);
			for (LoadSource source : sources) {
				int location = source.getLocation();
				String delim = source.getDelim();
				String sourceType = config.getProperty("source." + location
						+ ".type");
				if (!sourceType.equalsIgnoreCase("s3")) {
					String errorMessage = "Unsupported source type: "
							+ sourceType + " (location: " + location + ")";
					OtterException e = new OtterException(errorMessage);
					e.setEntity("source." + location + ".type");
					e.setStatus(400);
					throw e;
				}
				String s3Bucket = config.getProperty("source." + location
						+ ".bucket");
				String accessKey = config.getProperty("source." + location
						+ ".access");
				String secretKey = config.getProperty("source." + location
						+ ".secret");
				String path = source.getPath();
				cdhc.loadDataFromS3(s3Bucket, path, accessKey, secretKey,
						dsName);
			}
			imp.refreshTable(dsName);
			long rowsAfter = imp.getCount(Config.getInstance()
					.getImpalaDbName() + "." + dsName);
			map.put("rows_before", rowsBefore);
			map.put("rows_after", rowsAfter);
		} catch (SQLException sqle) {
			throw new OtterException(sqle);
		} catch (ClassNotFoundException cnfe) {
			throw new OtterException(cnfe);
		}
		return map;
	}

	@PUT
	@Path("/async_load/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	@Consumes(MediaType.APPLICATION_JSON)
	public Map asyncUploadDataset(@PathParam("id") String id,
			List<LoadSource> sources) throws OtterException {
		OfficeDb db = OfficeDb.getInstance();
		Impala imp = Impala.getInstance();
		CdhConnection cdhc = CdhConnection.getInstance();

		Map map = new HashMap();
		Config config = Config.getInstance();
		try {
			Dataset ds = db.getDataset(Integer.parseInt(id));
			String dsName = ds.getName();
			long rowsBefore = imp.getCount(Config.getInstance()
					.getImpalaDbName() + "." + dsName);
			for (LoadSource source : sources) {
				int location = source.getLocation();

				String sourceType = config.getProperty("source." + location
						+ ".type");
				if (!sourceType.equalsIgnoreCase("s3")) {
					String errorMessage = "Unsupported source type: "
							+ sourceType + " (location: " + location + ")";
					OtterException e = new OtterException(errorMessage);
					e.setEntity("source." + location + ".type");
					e.setStatus(400);
					throw e;
				}
				String s3Bucket = config.getProperty("source." + location
						+ ".bucket");
				String accessKey = config.getProperty("source." + location
						+ ".access");
				String secretKey = config.getProperty("source." + location
						+ ".secret");
				String path = source.getPath();
				cdhc.loadDataFromS3(s3Bucket, path, accessKey, secretKey,
						dsName);
			}
			imp.refreshTable(dsName);
			long rowsAfter = imp.getCount(Config.getInstance()
					.getImpalaDbName() + "." + dsName);
			map.put("rows_before", rowsBefore);
			map.put("rows_after", rowsAfter);
		} catch (SQLException sqle) {
			throw new OtterException(sqle);
		} catch (ClassNotFoundException cnfe) {
			throw new OtterException(cnfe);
		}
		return map;
	}

	@DELETE
	@Path("{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public void delete(@PathParam("id") int id) throws OtterException {
		OfficeDb db = OfficeDb.getInstance();
		Dataset ds;
		try {
			ds = db.getDataset(id);
			String dsName = ds.getName();
			CdhConnection.getInstance().deleteDataset(dsName);
			Impala.getInstance().refreshTable(dsName);
		} catch (SQLException e) {
			throw new OtterException(e);
		}
	}
}

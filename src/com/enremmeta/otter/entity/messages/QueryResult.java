package com.enremmeta.otter.entity.messages;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;

public class QueryResult implements OtterMessage {

	public QueryResult() {
		// TODO Auto-generated constructor stub
	}
	
	@JsonProperty("legacy_query_result")
	private Map legacyQueryResult;
	
	
}

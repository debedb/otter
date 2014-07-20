package com.enremmeta.otter;

import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class OtterExceptionMapper implements ExceptionMapper<OtterException> {

	@Context
	private HttpHeaders headers;

	public Response toResponse(OtterException e) {
		ResponseBuilder builder = Response.status(e.getStatus());
		builder = builder
				.entity(new OtterErrorResponseConverter(e));
		builder = builder.type(headers.getMediaType());
		Response resp = builder.build();
		return resp;
	}
}
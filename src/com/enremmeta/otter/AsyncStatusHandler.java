package com.enremmeta.otter;

import com.enremmeta.otter.entity.messages.OtterMessage;

public interface AsyncStatusHandler {
	public void handle(OtterMessage msg);
}

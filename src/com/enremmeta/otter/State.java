package com.enremmeta.otter;

import java.util.ArrayList;
import java.util.List;

public class State {
	public State() {
		super();
	}

	public void addJob(Job j) {
		jobs.add(j);
	}
	
	private List<Job> jobs = new ArrayList<Job>();
}

package com.enremmeta.otter;

public class TargetException extends Exception {

	private Throwable target;

	public TargetException(Throwable target) {
		super();
		this.target = target;
	}

	public Throwable getTarget() {
		return this.target;
	}

}

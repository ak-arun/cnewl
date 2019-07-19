package com.ak.hive.ddlgrabber.onetimegrabber.exceptions;

import com.ak.hive.ddlgrabber.onetimegrabber.util.DDLGrabberUtils;



public class DBException extends Exception{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public DBException (Exception e){
		super(DDLGrabberUtils.getTraceString(e));
	}
	
}

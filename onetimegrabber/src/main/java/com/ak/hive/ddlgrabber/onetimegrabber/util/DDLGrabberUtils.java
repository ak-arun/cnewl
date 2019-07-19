package com.ak.hive.ddlgrabber.onetimegrabber.util;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Properties;

public class DDLGrabberUtils {
	private static StringWriter sw = null;
	public static String getTraceString(Exception e){
		sw = new StringWriter();
		e.printStackTrace(new PrintWriter(sw));
		return sw.toString();
	}
	
	public static void loadProperty (Properties properties, String fileName) throws FileNotFoundException, IOException{
		properties = new Properties();
		properties.load(new FileReader(new File(fileName) ));
	}
	
}

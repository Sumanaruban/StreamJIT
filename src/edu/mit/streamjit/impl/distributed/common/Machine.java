package edu.mit.streamjit.impl.distributed.common;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Properties;

/**
 * Processor details of the machine on which StreamJIT is running. User need to
 * manually fill the machine details. In Linux systems, user may run 'lscpu'
 * command to get these information.
 * 
 * @author sumanan
 * @since 24 Nov, 2015
 */
public final class Machine {

	public static final int CPUs;

	public static final int threadsPerCore;

	public static final int sockets;

	public static final int coresPerSocket;

	public static final boolean isHTEnabled;

	public static final int physicalCores;

	static {
		Properties prop = loadProperties();
		int availP = Runtime.getRuntime().availableProcessors();
		CPUs = Integer.parseInt(prop.getProperty("CPUs"));
		threadsPerCore = Integer.parseInt(prop.getProperty("threadsPerCore"));
		sockets = Integer.parseInt(prop.getProperty("sockets"));
		coresPerSocket = Integer.parseInt(prop.getProperty("coresPerSocket"));
		if (sockets * coresPerSocket * threadsPerCore != CPUs)
			throw new IllegalStateException(
					String.format(
							"Machine details does not match. sockets(%d) * coresPerSocket(%d) * threadsPerCore(%d) != CPUs(%d)",
							sockets, coresPerSocket, threadsPerCore, CPUs));
		if (availP < CPUs)
			throw new IllegalStateException(
					String.format(
							"Available processors(%d) on this machine is lesser than number of CPUs(%d) asked to use.",
							availP, CPUs));
		isHTEnabled = !(threadsPerCore == 1);
		physicalCores = sockets * coresPerSocket;
	}

	public static Properties getProperties() {
		Properties prop = new Properties();
		setProperty(prop, "CPUs", CPUs);
		setProperty(prop, "threadsPerCore", threadsPerCore);
		setProperty(prop, "sockets", sockets);
		setProperty(prop, "coresPerSocket", coresPerSocket);
		return prop;
	}

	private static Properties loadProperties() {
		Properties prop = new Properties();
		InputStream input = null;
		try {
			input = new FileInputStream("machine.properties");
			prop.load(input);
		} catch (IOException ex) {
			System.err.println("Failed to load machine.properties");
		}
		return prop;
	}

	private static void setProperty(Properties prop, String name, Integer val) {
		prop.setProperty(name, val.toString());
	}

	public static void storeProperties() {
		OutputStream output = null;
		try {
			output = new FileOutputStream("machine.properties");
			Properties prop = getProperties();
			prop.store(output, null);
		} catch (IOException io) {
			io.printStackTrace();
		} finally {
			if (output != null) {
				try {
					output.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
}

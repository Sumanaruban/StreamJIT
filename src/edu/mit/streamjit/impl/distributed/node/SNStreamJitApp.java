package edu.mit.streamjit.impl.distributed.node;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.File;
import java.io.IOException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;

import com.google.common.base.Strings;

import edu.mit.streamjit.api.OneToOneElement;
import edu.mit.streamjit.api.Worker;
import edu.mit.streamjit.impl.common.Configuration;
import edu.mit.streamjit.impl.common.ConnectWorkersVisitor;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionProvider;
import edu.mit.streamjit.impl.distributed.common.Error;
import edu.mit.streamjit.impl.distributed.common.GlobalConstants;
import edu.mit.streamjit.impl.distributed.common.NetworkInfo;
import edu.mit.streamjit.impl.distributed.common.SNMessageElement.SNMessageElementHolder;
import edu.mit.streamjit.util.json.Jsonifiers;

/**
 * Represents a StreamJit application at {@link StreamNode} side. Controller
 * uses {@link edu.mit.streamjit.impl.distributed.controller.StreamJitApp}, which keeps
 * more details about the application. We can possibly have a base class and two
 * different children to represent apps at controller side and stream node side.
 * For the moment, lets have it separately.
 * 
 * @author sumanan
 * @since 20 Oct, 2015
 */
public class SNStreamJitApp {

	public final Configuration staticConfig;
	public final ConnectionProvider conProvider;
	public final String topLevelClass;
	public final OneToOneElement<?, ?> streamGraph;
	public final Worker<?, ?> source;
	private final StreamNode streamNode;
	public final int starterType;
	public final String appName;

	SNStreamJitApp(String json, StreamNode streamNode) {
		this.streamNode = streamNode;
		this.staticConfig = Jsonifiers.fromJson(json, Configuration.class);

		Map<Integer, InetAddress> iNetAddressMap = (Map<Integer, InetAddress>) staticConfig
				.getExtraData(GlobalConstants.INETADDRESS_MAP);

		NetworkInfo networkInfo = new NetworkInfo(iNetAddressMap);

		this.conProvider = new ConnectionProvider(streamNode.getNodeID(),
				networkInfo);
		appName = (String) staticConfig.getExtraData(GlobalConstants.APP_NAME);
		streamNode.createEventTimeLogger(appName);
		topLevelClass = (String) staticConfig
				.getExtraData(GlobalConstants.TOPLEVEL_WORKER_NAME);
		streamGraph = getStreamGraph();
		source = source();
		starterType = starterType();
	}

	int starterType() {
		Configuration.IntParameter param = staticConfig.getParameter(
				GlobalConstants.StarterType, Configuration.IntParameter.class);
		return param.getValue();
	}

	/**
	 * Gets a Stream Graph from a jar file.
	 * 
	 * @return : StreamGraph
	 */
	private OneToOneElement<?, ?> getStreamGraph() {
		ClassLoader loader = classLoader();
		if (loader == null)
			return null;

		String topStreamClassName = (String) staticConfig
				.getExtraData(GlobalConstants.TOPLEVEL_WORKER_NAME);
		checkNotNull(topStreamClassName);

		// In some benchmarks, top level stream class is written as an static
		// inner class. So in that case, we have to find the outer
		// class first. Java's Class.getName() returns "OutterClass$InnerClass"
		// format. So if $ exists in the method argument
		// topStreamClassName then the actual top level stream class is lies
		// inside another class.
		String outterClassName = null;
		if (topStreamClassName.contains("$")) {
			int pos = topStreamClassName.indexOf("$");
			outterClassName = (String) topStreamClassName.subSequence(0, pos);
			topStreamClassName = topStreamClassName.substring(pos + 1);
		}

		try {
			Class<?> topStreamClass;
			if (!Strings.isNullOrEmpty(outterClassName)) {
				Class<?> clazz1 = loader.loadClass(outterClassName);
				topStreamClass = getInngerClass(clazz1, topStreamClassName);
			} else {
				topStreamClass = loader.loadClass(topStreamClassName);
			}
			System.out.println(topStreamClass.getSimpleName());
			return (OneToOneElement<?, ?>) topStreamClass.newInstance();
		} catch (InstantiationException iex) {
			System.err.println("InstantiationException exception.");
			System.err
					.println("Please ensure the top level StreamJit application"
							+ " class is public and have no argument constructor.");
			iex.printStackTrace();
		} catch (Exception e) {
			System.out.println("Couldn't find the toplevel worker.");
			e.printStackTrace();

			// TODO: Try catch inside a catch block. Good practice???
			try {
				streamNode.controllerConnection
						.writeObject(new SNMessageElementHolder(
								Error.WORKER_NOT_FOUND, 0));
			} catch (IOException e1) {
				e1.printStackTrace();
			}
		}
		return null;
	}

	private static Class<?> getInngerClass(Class<?> OutterClass,
			String InnterClassName) throws ClassNotFoundException {
		Class<?>[] kl = OutterClass.getClasses();
		for (Class<?> k : kl) {
			if (InnterClassName.equals(k.getSimpleName())) {
				return k;
			}
		}
		throw new ClassNotFoundException(
				String.format(
						"Innter class %s is not found in the outter class %s. Check the accessibility/visibility of the inner class",
						InnterClassName, OutterClass.getName()));
	}

	private ClassLoader classLoader() {
		String jarFilePath = (String) staticConfig
				.getExtraData(GlobalConstants.JARFILE_PATH);

		checkNotNull(jarFilePath);
		jarFilePath = this.getClass().getProtectionDomain().getCodeSource()
				.getLocation().getPath();
		File jarFile = new java.io.File(jarFilePath);
		if (!jarFile.exists()) {
			System.out.println("Jar file not found....");
			try {
				streamNode.controllerConnection
						.writeObject(new SNMessageElementHolder(
								Error.FILE_NOT_FOUND, 0));
			} catch (IOException e) {
				e.printStackTrace();
			}
			return null;
		}

		try {
			URL url = jarFile.toURI().toURL();
			URL[] urls = new URL[] { url };
			ClassLoader loader = new URLClassLoader(urls);
			return loader;
		} catch (MalformedURLException e) {
			e.printStackTrace();
			System.out.println("Couldn't find the toplevel worker...Exiting");

			// TODO: Try catch inside a catch block. Good practice???
			try {
				streamNode.controllerConnection
						.writeObject(new SNMessageElementHolder(
								Error.WORKER_NOT_FOUND, 0));
			} catch (IOException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			// System.exit(0);
		}
		return null;
	}

	private Worker<?, ?> source() {
		ConnectWorkersVisitor primitiveConnector = new ConnectWorkersVisitor();
		streamGraph.visit(primitiveConnector);
		return primitiveConnector.getSource();
	}
}

/**
 * @author Sumanan sumanan@mit.edu
 * @since May 20, 2013
 */
package edu.mit.streamjit.impl.distributed.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;

import edu.mit.streamjit.impl.distributed.common.AsynchronousTCPConnection.AsyncTCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.TCPConnection.TCPConnectionInfo;
import edu.mit.streamjit.impl.distributed.common.Connection.ConnectionInfo;

public class Tester {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// test1();
		test2();
	}

	/**
	 * Testing one - tests the size of an object.
	 */
	private static void test1() {
		Error er = Error.FILE_NOT_FOUND;
		AppStatus apSts = AppStatus.STOPPED;
		ByteArrayOutputStream byteAos = new ByteArrayOutputStream();

		ObjectOutputStream os = null;
		try {
			os = new ObjectOutputStream(byteAos);
		} catch (IOException e) {
			e.printStackTrace();
		}

		try {
			os.writeObject(apSts);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			os.writeInt(34345);
		} catch (IOException e) {
			e.printStackTrace();
		}

		System.out.println(byteAos.toByteArray().length);
	}

	/**
	 * Tests the equals and hascode.
	 */
	private static void test2() {

		ConnectionInfo asyConInfo1 = new AsyncTCPConnectionInfo(1, 4, 8989);
		ConnectionInfo asyConInfo2 = new AsyncTCPConnectionInfo(1, 4, 8980);
		ConnectionInfo asyConInfo3 = new AsyncTCPConnectionInfo(4, 1, 8989);
		ConnectionInfo asyConInfo4 = new AsyncTCPConnectionInfo(4, 1, 8980);
		ConnectionInfo asyConInfo5 = new AsyncTCPConnectionInfo(1, 4, 8989);

		ConnectionInfo tcpConInfo1 = new TCPConnectionInfo(1, 4, 8989);
		ConnectionInfo tcpConInfo2 = new TCPConnectionInfo(1, 4, 8980);
		ConnectionInfo tcpConInfo3 = new TCPConnectionInfo(4, 1, 8989);
		ConnectionInfo tcpConInfo4 = new TCPConnectionInfo(4, 1, 8980);

		ConnectionInfo conInfo1 = new ConnectionInfo(1, 4, true);
		ConnectionInfo conInfo2 = new ConnectionInfo(1, 4, false);
		ConnectionInfo conInfo3 = new ConnectionInfo(4, 1, true);
		ConnectionInfo conInfo4 = new ConnectionInfo(4, 1, false);

		System.out.println("AsyncTCPConnectionInfo - AsyncTCPConnectionInfo");
		System.out.println(asyConInfo1.equals(asyConInfo2));
		System.out.println(asyConInfo1.equals(asyConInfo3));
		System.out.println(asyConInfo1.equals(asyConInfo4));
		System.out.println(asyConInfo2.equals(asyConInfo3));
		System.out.println(asyConInfo2.equals(asyConInfo4));
		System.out.println(asyConInfo3.equals(asyConInfo4));
		System.out.println();

		System.out.println("ConnectionInfo - AsyncTCPConnectionInfo");
		System.out.println(conInfo1.equals(asyConInfo1));
		System.out.println(conInfo1.equals(asyConInfo2));
		System.out.println(conInfo1.equals(asyConInfo3));
		System.out.println(conInfo1.equals(asyConInfo4));
		System.out.println(conInfo2.equals(asyConInfo1));
		System.out.println(conInfo2.equals(asyConInfo2));
		System.out.println(conInfo2.equals(asyConInfo3));
		System.out.println(conInfo2.equals(asyConInfo4));
		System.out.println(conInfo3.equals(asyConInfo1));
		System.out.println(conInfo3.equals(asyConInfo2));
		System.out.println(conInfo3.equals(asyConInfo3));
		System.out.println(conInfo3.equals(asyConInfo4));
		System.out.println(conInfo4.equals(asyConInfo1));
		System.out.println(conInfo4.equals(asyConInfo2));
		System.out.println(conInfo4.equals(asyConInfo3));
		System.out.println(conInfo4.equals(asyConInfo4));
		System.out.println();

		System.out.println("ConnectionInfo - TCPConnectionInfo");
		System.out.println(conInfo1.equals(tcpConInfo1));
		System.out.println(conInfo1.equals(tcpConInfo2));
		System.out.println(conInfo1.equals(tcpConInfo3));
		System.out.println(conInfo1.equals(tcpConInfo4));
		System.out.println(conInfo2.equals(tcpConInfo1));
		System.out.println(conInfo2.equals(tcpConInfo2));
		System.out.println(conInfo2.equals(tcpConInfo3));
		System.out.println(conInfo2.equals(tcpConInfo4));
		System.out.println(conInfo3.equals(tcpConInfo1));
		System.out.println(conInfo3.equals(tcpConInfo2));
		System.out.println(conInfo3.equals(tcpConInfo3));
		System.out.println(conInfo3.equals(tcpConInfo4));
		System.out.println(conInfo4.equals(tcpConInfo1));
		System.out.println(conInfo4.equals(tcpConInfo2));
		System.out.println(conInfo4.equals(tcpConInfo3));
		System.out.println(conInfo4.equals(tcpConInfo4));
		System.out.println();

		Map<ConnectionInfo, Integer> tesMap = new HashMap<>();
		tesMap.put(tcpConInfo1, 1);
		tesMap.put(asyConInfo1, 2);

		System.out.println(tesMap.containsKey(tcpConInfo1));
		System.out.println(tesMap.containsKey(tcpConInfo2));
		System.out.println(tesMap.containsKey(tcpConInfo3));
		System.out.println(tesMap.containsKey(tcpConInfo4));

		System.out.println(tesMap.containsKey(asyConInfo1));
		System.out.println(tesMap.containsKey(asyConInfo2));
		System.out.println(tesMap.containsKey(asyConInfo3));
		System.out.println(tesMap.containsKey(asyConInfo4));
		System.out.println(tesMap.containsKey(asyConInfo5));

		System.out.println(tesMap.containsKey(conInfo1));
		System.out.println(tesMap.containsKey(conInfo2));
		System.out.println(tesMap.containsKey(conInfo3));
		System.out.println(tesMap.containsKey(conInfo4));
	}

}

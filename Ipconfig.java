import java.net.*;
import java.util.*;
/**
 * <p>Lists the IP interfaces defined on the system using the Java NetworkInterface.</p>
 * <p>alexis.grandemange@pagebox.net</p>
 * <p>Copyright (c) 2003 Alexis Grandemange</p>
 * <pre>This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public License
 * as published by the Free Software Foundation; version 2.1 of the
 * License.
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
 * GNU Lesser General Public License for more details.
 * A copy of the GNU Lesser General Public License lesser.txt should be
 * included in the distribution.</pre>
 * @author  Alexis Grandemange
 * @version 0, 0, 1
 */
public class Ipconfig {
	Ipconfig() throws Exception {
		Enumeration<NetworkInterface> en = NetworkInterface.getNetworkInterfaces();
		while(en.hasMoreElements()) {
			NetworkInterface ni = en.nextElement();
			Enumeration<InetAddress> en2 = ni.getInetAddresses();
			while(en2.hasMoreElements()) {
				InetAddress ia = en2.nextElement();
				System.out.println("Interface name:" + ni.getName() +
					" display name:" + ni.getDisplayName() + " " +
					ia.getHostAddress());
			}
		}
	}
	public static void main(String[] args) throws Exception {
		new Ipconfig();
	}
}

/**
 *  Copyright 2008 ThimbleWare Inc.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *  
 *  Fork copyright 2015 by hdsdi3g for http://hd3g.tv/
 */
package com.thimbleware.jmemcached;

import java.net.InetSocketAddress;

import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;

/**
 * Command line interface to the Java memcache daemon.
 */
public class MainCli {
	
	public static void main(String[] args) throws Exception {
		System.out.println("# jmemcache-daemon");
		System.out.println("# Copyright 2008 ThimbleWare Inc.");
		System.out.println("# Fork copyright 2015 by hdsdi3g for http://hd3g.tv/");
		System.out.println("# https://github.com/hdsdi3g/jmemcache-daemon");
		System.out.println("#");
		
		int port = Integer.parseInt(System.getProperty("port", "11211"));
		InetSocketAddress addr = new InetSocketAddress(System.getProperty("addr", "127.0.0.1"), port);
		int max_size = Integer.parseInt(System.getProperty("maxsize", "1000000"));
		int idle = Integer.parseInt(System.getProperty("idle", "-1")); // disconnect after idle <x> seconds
		boolean verbose = Boolean.parseBoolean(System.getProperty("verbose", "false"));
		int max_bytes = Integer.parseInt(System.getProperty("maxbytes", String.valueOf(Runtime.getRuntime().maxMemory()))); // Setting max memory size to JVM limit
		
		if (max_bytes > Runtime.getRuntime().maxMemory()) {
			System.err.println("ERROR : JVM heap size is not big enough. use '-Xmx" + String.valueOf(max_bytes / 1024000) + "m' java argument before the '-jar' option.");
			System.exit(1);
		}
		
		if (verbose) {
			if (idle > 0) {
				System.out.println("# Set idle=       " + idle);
			}
			System.out.println("# Set max_size=   " + max_size);
			System.out.println("# Set max_bytes=  " + max_bytes);
			System.out.println("# VM maxMemory=   " + Runtime.getRuntime().maxMemory());
			System.out.println("# VM freeMemory=  " + Runtime.getRuntime().freeMemory());
			System.out.println("# VM totalMemory= " + Runtime.getRuntime().totalMemory());
			System.out.println("#");
		}
		
		final MemCacheDaemon daemon = new MemCacheDaemon();
		ConcurrentLinkedHashMap storage = new ConcurrentLinkedHashMap(max_size, max_bytes);
		daemon.setCache(new Cache(storage));
		daemon.setAddr(addr);
		daemon.setIdleTime(idle);
		daemon.setVerbose(verbose);
		daemon.start();
		
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				if (daemon.isRunning()) daemon.stop();
			}
		}));
	}
}

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
 *  Forked by hdsdi3g (https://github.com/hdsdi3g) http://hd3g.tv
 *  
 */
package com.thimbleware.jmemcached;

import java.net.InetSocketAddress;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;

import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import com.thimbleware.jmemcached.util.Bytes;

/**
 * Command line interface to the Java memcache daemon.
 * Arguments in general parallel those of the C implementation.
 */
public class MainCli {
	
	public static void main(String[] args) throws Exception {
		// look for external log4j.properties
		
		// setup command line options
		Options options = new Options();
		options.addOption("h", "help", false, "print this help screen");
		options.addOption("i", "idle", true, "disconnect after idle <x> seconds");
		options.addOption("m", "memory", true, "max memory to use; in bytes, specify K, kb, M, GB for larger units");
		options.addOption("c", "ceiling", true, "ceiling memory to use; in bytes, specify K, kb, M, GB for larger units");
		options.addOption("s", "size", true, "max items");
		options.addOption("b", "binary", false, "binary protocol mode");
		options.addOption("V", false, "Show version number");
		options.addOption("v", false, "verbose (show commands)");
		
		// read command line options
		CommandLineParser parser = new PosixParser();
		CommandLine cmdline = parser.parse(options, args);
		
		int port = Integer.parseInt(System.getProperty("port", "11211"));
		InetSocketAddress addr = new InetSocketAddress(System.getProperty("addr", "127.0.0.1"), port);
		
		int max_size = 1000000;
		if (cmdline.hasOption("s"))
			max_size = (int) Bytes.valueOf(cmdline.getOptionValue("s")).bytes();
		else if (cmdline.hasOption("size")) max_size = (int) Bytes.valueOf(cmdline.getOptionValue("size")).bytes();
		
		System.out.println("Setting max cache elements to " + String.valueOf(max_size));
		
		int idle = -1;
		if (cmdline.hasOption("i")) {
			idle = Integer.parseInt(cmdline.getOptionValue("i"));
		} else if (cmdline.hasOption("idle")) {
			idle = Integer.parseInt(cmdline.getOptionValue("idle"));
		}
		
		boolean verbose = false;
		if (cmdline.hasOption("v")) {
			verbose = true;
		}
		
		long ceiling;
		if (cmdline.hasOption("c")) {
			ceiling = Bytes.valueOf(cmdline.getOptionValue("c")).bytes();
			System.out.println("Setting ceiling memory size to " + Bytes.bytes(ceiling).megabytes() + "M");
		} else if (cmdline.hasOption("ceiling")) {
			ceiling = Bytes.valueOf(cmdline.getOptionValue("ceiling")).bytes();
			System.out.println("Setting ceiling memory size to " + Bytes.bytes(ceiling).megabytes() + "M");
		} else {
			ceiling = 1024000;
			System.out.println("Setting ceiling memory size to default limit of " + Bytes.bytes(ceiling).megabytes() + "M");
		}
		
		long maxBytes;
		if (cmdline.hasOption("m")) {
			maxBytes = Bytes.valueOf(cmdline.getOptionValue("m")).bytes();
			System.out.println("Setting max memory size to " + Bytes.bytes(maxBytes).gigabytes() + "GB");
		} else if (cmdline.hasOption("memory")) {
			maxBytes = Bytes.valueOf(cmdline.getOptionValue("memory")).bytes();
			System.out.println("Setting max memory size to " + Bytes.bytes(maxBytes).gigabytes() + "GB");
		} else {
			maxBytes = Runtime.getRuntime().maxMemory();
			System.out.println("Setting max memory size to JVM limit of " + Bytes.bytes(maxBytes).gigabytes() + "GB");
		}
		
		if (maxBytes > Runtime.getRuntime().maxMemory()) {
			System.out.println("ERROR : JVM heap size is not big enough. use '-Xmx" + String.valueOf(maxBytes / 1024000) + "m' java argument before the '-jar' option.");
			return;
		} else if (maxBytes > Integer.MAX_VALUE) {
			System.out.println("ERROR : when external memory mapped, memory size may not exceed the size of Integer.MAX_VALUE (" + Bytes.bytes(Integer.MAX_VALUE).gigabytes() + "GB");
			return;
		}
		
		// create daemon and start it
		final MemCacheDaemon daemon = new MemCacheDaemon();
		
		ConcurrentLinkedHashMap<Key> storage = ConcurrentLinkedHashMap.create(ConcurrentLinkedHashMap.EvictionPolicy.FIFO, max_size, maxBytes);
		
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

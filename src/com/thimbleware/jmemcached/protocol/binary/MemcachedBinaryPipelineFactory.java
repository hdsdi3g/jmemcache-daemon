package com.thimbleware.jmemcached.protocol.binary;

import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.group.DefaultChannelGroup;

import com.thimbleware.jmemcached.Cache;
import com.thimbleware.jmemcached.protocol.MemcachedCommandHandler;

public class MemcachedBinaryPipelineFactory implements ChannelPipelineFactory {
	
	private final MemcachedBinaryCommandDecoder decoder = new MemcachedBinaryCommandDecoder();
	@SuppressWarnings("rawtypes")
	private final MemcachedCommandHandler memcachedCommandHandler;
	@SuppressWarnings("rawtypes")
	private final MemcachedBinaryResponseEncoder memcachedBinaryResponseEncoder = new MemcachedBinaryResponseEncoder();
	
	@SuppressWarnings("rawtypes")
	public MemcachedBinaryPipelineFactory(Cache cache, String version, boolean verbose, int idleTime, DefaultChannelGroup channelGroup) {
		memcachedCommandHandler = new MemcachedCommandHandler(cache, version, verbose, idleTime, channelGroup);
	}
	
	@Override
	public ChannelPipeline getPipeline() throws Exception {
		return Channels.pipeline(decoder, memcachedCommandHandler, memcachedBinaryResponseEncoder);
	}
}

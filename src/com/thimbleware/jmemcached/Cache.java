package com.thimbleware.jmemcached;

import static java.lang.String.valueOf;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.Delayed;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.buffer.ChannelBuffers;

import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;

public final class Cache {
	/**
	 * Enum defining response statuses from set/add type commands
	 */
	public enum StoreResponse {
		STORED, NOT_STORED, EXISTS, NOT_FOUND
	}
	
	/**
	 * Enum defining responses statuses from removal commands
	 */
	public enum DeleteResponse {
		DELETED, NOT_FOUND
	}
	
	final ConcurrentLinkedHashMap<Key> storage;
	final DelayQueue<DelayedMCElement> deleteQueue;
	private final ScheduledExecutorService scavenger;
	
	public Cache(ConcurrentLinkedHashMap<Key> storage) {
		started.set(System.currentTimeMillis());
		// getCmds.set(0);
		// setCmds.set(0);
		// getHits.set(0);
		// getMisses.set(0);
		
		this.storage = storage;
		deleteQueue = new DelayQueue<DelayedMCElement>();
		
		scavenger = Executors.newScheduledThreadPool(1);
		scavenger.scheduleAtFixedRate(new Runnable() {
			@Override
			public void run() {
				asyncEventPing();
			}
		}, 10, 2, TimeUnit.SECONDS);
	}
	
	/**
	 * Handle the deletion of an item from the cache.
	 * @param key the key for the item
	 * @param time an amount of time to block this entry in the cache for further writes
	 * @return the message response
	 */
	public DeleteResponse delete(Key key, int time) {
		boolean removed = false;
		
		// delayed remove
		if (time != 0) {
			// block the element and schedule a delete; replace its entry with a blocked element
			CacheElement placeHolder = new CacheElement(key, 0, 0, 0L);
			placeHolder.setData(ChannelBuffers.buffer(0));
			placeHolder.block(Now() + (long) time);
			
			storage.replace(key, placeHolder);
			
			// this must go on a queue for processing later...
			deleteQueue.add(new DelayedMCElement(placeHolder));
		} else
			removed = storage.remove(key) != null;
		
		if (removed)
			return DeleteResponse.DELETED;
		else
			return DeleteResponse.NOT_FOUND;
		
	}
	
	/**
	 * Add an element to the cache
	 * @param e the element to add
	 * @return the store response code
	 */
	public StoreResponse add(CacheElement e) {
		final long origCasUnique = e.getCasUnique();
		e.setCasUnique(casCounter.getAndIncrement());
		final boolean stored = storage.putIfAbsent(e.getKey(), e) == null;
		// we should restore the former cas so that the object isn't left dirty
		if (!stored) {
			e.setCasUnique(origCasUnique);
		}
		return stored ? StoreResponse.STORED : StoreResponse.NOT_STORED;
	}
	
	/**
	 * Replace an element in the cache
	 * @param e the element to replace
	 * @return the store response code
	 */
	public StoreResponse replace(CacheElement e) {
		return storage.replace(e.getKey(), e) != null ? StoreResponse.STORED : StoreResponse.NOT_STORED;
	}
	
	/**
	 * Append bytes to the end of an element in the cache
	 * @param element the element to append
	 * @return the store response code
	 */
	public StoreResponse append(CacheElement element) {
		CacheElement old = storage.get(element.getKey());
		if (old == null || isBlocked(old) || isExpired(old)) {
			getMisses.incrementAndGet();
			return StoreResponse.NOT_FOUND;
		} else {
			return storage.replace(old.getKey(), old, old.append(element)) ? StoreResponse.STORED : StoreResponse.NOT_STORED;
		}
	}
	
	/**
	 * Prepend bytes to the end of an element in the cache
	 * @param element the element to append
	 * @return the store response code
	 */
	public StoreResponse prepend(CacheElement element) {
		CacheElement old = storage.get(element.getKey());
		if (old == null || isBlocked(old) || isExpired(old)) {
			getMisses.incrementAndGet();
			return StoreResponse.NOT_FOUND;
		} else {
			return storage.replace(old.getKey(), old, old.prepend(element)) ? StoreResponse.STORED : StoreResponse.NOT_STORED;
		}
	}
	
	/**
	 * Set an element in the cache
	 * @param e the element to set
	 * @return the store response code
	 */
	public StoreResponse set(CacheElement e) {
		setCmds.incrementAndGet();// update stats
		
		e.setCasUnique(casCounter.getAndIncrement());
		
		storage.put(e.getKey(), e);
		
		return StoreResponse.STORED;
	}
	
	/**
	 * Set an element in the cache but only if the element has not been touched
	 * since the last 'gets'
	 * @param cas_key the cas key returned by the last gets
	 * @param e the element to set
	 * @return the store response code
	 */
	public StoreResponse cas(Long cas_key, CacheElement e) {
		// have to get the element
		CacheElement element = storage.get(e.getKey());
		if (element == null || isBlocked(element)) {
			getMisses.incrementAndGet();
			return StoreResponse.NOT_FOUND;
		}
		
		if (element.getCasUnique() == cas_key) {
			// casUnique matches, now set the element
			e.setCasUnique(casCounter.getAndIncrement());
			if (storage.replace(e.getKey(), element, e))
				return StoreResponse.STORED;
			else {
				getMisses.incrementAndGet();
				return StoreResponse.NOT_FOUND;
			}
		} else {
			// cas didn't match; someone else beat us to it
			return StoreResponse.EXISTS;
		}
	}
	
	/**
	 * Increment/decremen t an (integer) element in the cache
	 * @param key the key to increment
	 * @param mod the amount to add to the value
	 * @return the message response
	 */
	public Integer get_add(Key key, int mod) {
		CacheElement old = storage.get(key);
		if (old == null || isBlocked(old) || isExpired(old)) {
			getMisses.incrementAndGet();
			return null;
		} else {
			CacheElement.IncrDecrResult result = old.add(mod);
			return storage.replace(old.getKey(), old, result.replace) ? result.oldValue : null;
		}
	}
	
	protected boolean isBlocked(CacheElement e) {
		return e.isBlocked() && e.getBlockedUntil() > Now();
	}
	
	protected boolean isExpired(CacheElement e) {
		return e.getExpire() != 0 && e.getExpire() < Now();
	}
	
	/**
	 * Get element(s) from the cache
	 * @param keys the key for the element to lookup
	 * @return the element, or 'null' in case of cache miss.
	 */
	public CacheElement[] get(Key... keys) {
		getCmds.incrementAndGet();// updates stats
		
		CacheElement[] elements = new CacheElement[keys.length];
		int x = 0;
		int hits = 0;
		int misses = 0;
		for (Key key : keys) {
			CacheElement e = storage.get(key);
			if (e == null || isExpired(e) || e.isBlocked()) {
				misses++;
				
				elements[x] = null;
			} else {
				hits++;
				
				elements[x] = e;
			}
			x++;
			
		}
		getMisses.addAndGet(misses);
		getHits.addAndGet(hits);
		
		return elements;
		
	}
	
	/**
	 * Flush all cache entries
	 * @return command response
	 */
	public boolean flush_all() {
		return flush_all(0);
	}
	
	/**
	 * Flush all cache entries with a timestamp after a given expiration time
	 * @param expire the flush time in seconds
	 * @return command response
	 */
	public boolean flush_all(int expire) {
		// T O D O implement this, it isn't right... but how to handle efficiently? (don't want to linear scan entire cacheStorage)
		storage.clear();
		return true;
	}
	
	/**
	 * Close the cache, freeing all resources on which it depends.
	 * @throws IOException
	 */
	public void close() throws IOException {
		scavenger.shutdown();
		;
		storage.close();
	}
	
	protected Set<Key> keys() {
		return storage.keySet();
	}
	
	/**
	 * @return the # of items in the cache
	 */
	public long getCurrentItems() {
		return storage.size();
	}
	
	/**
	 * @return the maximum size of the cache (in bytes)
	 */
	public long getLimitMaxBytes() {
		return storage.getMemoryCapacity();
	}
	
	/**
	 * @return the current cache usage (in bytes)
	 */
	public long getCurrentBytes() {
		return storage.getMemoryUsed();
	}
	
	/**
	 * Called periodically by the network event loop to process any pending events.
	 * (such as delete queues, etc.)
	 */
	public void asyncEventPing() {
		DelayedMCElement toDelete = deleteQueue.poll();
		if (toDelete != null) {
			storage.remove(toDelete.element.getKey());
		}
	}
	
	/**
	 * Delayed key blocks get processed occasionally.
	 */
	protected static class DelayedMCElement implements Delayed {
		private CacheElement element;
		
		public DelayedMCElement(CacheElement element) {
			this.element = element;
		}
		
		@Override
		public long getDelay(TimeUnit timeUnit) {
			return timeUnit.convert(element.getBlockedUntil() - Now(), TimeUnit.MILLISECONDS);
		}
		
		@Override
		public int compareTo(Delayed delayed) {
			if (!(delayed instanceof Cache.DelayedMCElement))
				return -1;
			else
				return element.getKey().toString().compareTo(((DelayedMCElement) delayed).element.getKey().toString());
		}
	}
	
	protected final AtomicLong started = new AtomicLong();
	
	protected final AtomicInteger getCmds = new AtomicInteger();
	protected final AtomicInteger setCmds = new AtomicInteger();
	protected final AtomicInteger getHits = new AtomicInteger();
	protected final AtomicInteger getMisses = new AtomicInteger();
	protected final AtomicLong casCounter = new AtomicLong(1);
	
	/**
	 * @return the current time in seconds (from epoch), used for expiries, etc.
	 */
	public static int Now() {
		return (int) (System.currentTimeMillis());
	}
	
	/**
	 * @return the number of get commands executed
	 */
	public final int getGetCmds() {
		return getCmds.get();
	}
	
	/**
	 * @return the number of set commands executed
	 */
	public final int getSetCmds() {
		return setCmds.get();
	}
	
	/**
	 * @return the number of get hits
	 */
	public final int getGetHits() {
		return getHits.get();
	}
	
	/**
	 * @return the number of stats
	 */
	public final int getGetMisses() {
		return getMisses.get();
	}
	
	/**
	 * Retrieve stats about the cache. If an argument is specified, a specific category of stats is requested.
	 * Return runtime statistics
	 * @param arg a specific extended stat sub-category, arg additional arguments to the stats command
	 * @return a map of stats, the full command response
	 */
	public final Map<String, Set<String>> stat(String arg) {
		Map<String, Set<String>> result = new HashMap<String, Set<String>>();
		
		// stats we know
		multiSet(result, "cmd_gets", valueOf(getGetCmds()));
		multiSet(result, "cmd_sets", valueOf(getSetCmds()));
		multiSet(result, "get_hits", valueOf(getGetHits()));
		multiSet(result, "get_misses", valueOf(getGetMisses()));
		multiSet(result, "time", valueOf(valueOf(Now())));
		multiSet(result, "uptime", valueOf(Now() - this.started.longValue()));
		multiSet(result, "cur_items", valueOf(this.getCurrentItems()));
		multiSet(result, "limit_maxbytes", valueOf(this.getLimitMaxBytes()));
		multiSet(result, "current_bytes", valueOf(this.getCurrentBytes()));
		multiSet(result, "free_bytes", valueOf(Runtime.getRuntime().freeMemory()));
		
		// Not really the same thing precisely, but meaningful nonetheless. potentially this should be renamed
		multiSet(result, "pid", valueOf(Thread.currentThread().getId()));
		
		// stuff we know nothing about; gets faked only because some clients expect this
		multiSet(result, "rusage_user", "0:0");
		multiSet(result, "rusage_system", "0:0");
		multiSet(result, "connection_structures", "0");
		
		// T O D O we could collect these stats
		multiSet(result, "bytes_read", "0");
		multiSet(result, "bytes_written", "0");
		
		return result;
	}
	
	private void multiSet(Map<String, Set<String>> map, String key, String val) {
		Set<String> cur = map.get(key);
		if (cur == null) {
			cur = new HashSet<String>();
		}
		cur.add(val);
		map.put(key, cur);
	}
	
}

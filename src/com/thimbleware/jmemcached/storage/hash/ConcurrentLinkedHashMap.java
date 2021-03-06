/*
 * Copyright 2009 Benjamin Manes
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.thimbleware.jmemcached.storage.hash;

import java.io.Serializable;
import java.util.AbstractCollection;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.thimbleware.jmemcached.CacheElement;
import com.thimbleware.jmemcached.Key;

/**
 * A {@link ConcurrentMap} with a doubly-linked list running through its entries.
 * <p/>
 * This class provides the same semantics as a {@link ConcurrentHashMap} in terms of iterators, acceptable keys, and concurrency characteristics, but perform slightly worse due to the added expense of
 * maintaining the linked list. It differs from {@link java.util.LinkedHashMap} in that it does not provide predictable iteration order.
 * <p/>
 * This map is intended to be used for caches and provides the following eviction policies:
 * <ul>
 * <li>First-in, First-out: Also known as insertion order. This policy has excellent concurrency characteristics and an adequate hit rate.
 * <li>Second-chance: An enhanced FIFO policy that marks entries that have been retrieved and saves them from being evicted until the next pass. This enhances the FIFO policy by making it aware of
 * "hot" entries, which increases its hit rate to be equal to an LRU's under normal workloads. In the worst case, where all entries have been saved, this policy degrades to a FIFO.
 * <li>Least Recently Used: An eviction policy based on the observation that entries that have been used recently will likely be used again soon. This policy provides a good approximation of an
 * optimal algorithm, but suffers by being expensive to maintain. The cost of reordering entries on the list during every access operation reduces the concurrency and performance characteristics of
 * this policy.
 * </ul>
 * @author <a href="mailto:ben.manes@reardencommerce.com">Ben Manes</a>
 * @see http://code.google.com/p/concurrentlinkedhashmap/
 */
public final class ConcurrentLinkedHashMap extends AbstractMap<Key, CacheElement> implements Serializable, ConcurrentMap<Key, CacheElement> {
	private static final EvictionListener nullListener = new EvictionListener() {
		
		@Override
		public void onEviction(Key key, com.thimbleware.jmemcached.CacheElement value) {
		}
	};
	private static final long serialVersionUID = 8350170357874293408L;
	final ConcurrentMap<Key, Node> data;
	final EvictionListener listener;
	final AtomicInteger capacity;
	final EvictionPolicy policy;
	final AtomicInteger length;
	final Node sentinel;
	final Lock lock;
	final AtomicLong memoryCapacity;
	final AtomicLong memoryUsed;
	
	/**
	 * Creates a map with the specified eviction policy, maximum capacity, eviction listener, and concurrency level.
	 * @param policy The eviction policy to apply when the size exceeds the maximum capacity.
	 * @param maximumCapacity The maximum capacity to coerces to. The size may exceed it temporarily.
	 * @param concurrencyLevel The estimated number of concurrently updating threads. The implementation
	 *        performs internal sizing to try to accommodate this many threads.
	 * @param listener The listener registered for notification when an entry is evicted.
	 */
	public ConcurrentLinkedHashMap(int maximumCapacity, long maximumMemoryCapacity) {
		if (maximumCapacity < 0) {
			throw new IllegalArgumentException();
		}
		this.data = new ConcurrentHashMap<Key, Node>(maximumCapacity, 0.75f, 16);
		this.capacity = new AtomicInteger(maximumCapacity);
		this.length = new AtomicInteger();
		this.listener = nullListener;
		this.policy = ConcurrentLinkedHashMap.EvictionPolicy.FIFO;
		this.lock = new ReentrantLock();
		this.sentinel = new Node(lock);
		this.memoryUsed = new AtomicLong(0);
		this.memoryCapacity = new AtomicLong(maximumMemoryCapacity);
	}
	
	/**
	 * Determines whether the map has exceeded its capacity.
	 * @return Whether the map has overflowed and an entry should be evicted.
	 */
	private boolean isOverflow() {
		return size() > capacity() || getMemoryUsed() > getMemoryCapacity();
	}
	
	public long getMemoryCapacity() {
		return memoryCapacity.get();
	}
	
	public long getMemoryUsed() {
		return memoryUsed.get();
	}
	
	/**
	 * Sets the maximum capacity of the map and eagerly evicts entries until it shrinks to the appropriate size.
	 * @param capacity The maximum capacity of the map.
	 */
	public void setCapacity(int capacity) {
		if (capacity < 0) {
			throw new IllegalArgumentException();
		}
		this.capacity.set(capacity);
		while (evict()) {
		}
	}
	
	/**
	 * Sets the maximum capacity of the map and eagerly evicts entries until it shrinks to the appropriate size.
	 * @param capacity The maximum capacity of the map.
	 */
	public void setMemoryCapacity(int capacity) {
		if (capacity < 0) {
			throw new IllegalArgumentException();
		}
		this.memoryCapacity.set(capacity);
		while (evict()) {
		}
	}
	
	/**
	 * Retrieves the maximum capacity of the map.
	 * @return The maximum capacity.
	 */
	public int capacity() {
		return capacity.get();
	}
	
	public void close() {
		clear();
	}
	
	/**
	 * {@inheritDoc}
	 */
	public int size() {
		int size = length.get();
		return (size >= 0) ? size : 0;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public void clear() {
		for (Key key : keySet()) {
			remove(key);
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean containsKey(Object key) {
		return data.containsKey(key);
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean containsValue(Object value) {
		if (value == null) {
			throw new IllegalArgumentException();
		}
		return data.containsValue(new Node(null, (CacheElement) value, null, lock));
	}
	
	/**
	 * Evicts a single entry if the map exceeds the maximum capacity.
	 */
	private boolean evict() {
		while (isOverflow()) {
			Node node = sentinel.getNext();
			if (node == sentinel) {
				return false;
			} else if (policy.onEvict(this, node)) {
				// Attempt to remove the node if it's still available
				if (data.remove(node.getKey(), new Identity(node))) {
					length.decrementAndGet();
					memoryUsed.addAndGet(-1 * node.getValue().size());
					node.remove();
					listener.onEviction(node.getKey(), node.getValue());
					return true;
				}
			}
		}
		return false;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public CacheElement get(Object key) {
		Node node = data.get(key);
		if (node != null) {
			policy.onAccess(this, node);
			return node.getValue();
		}
		return null;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public CacheElement put(Key key, CacheElement value) {
		if (value == null) {
			throw new IllegalArgumentException();
		}
		Node old = putIfAbsent(new Node(key, value, sentinel, lock));
		
		memoryUsed.addAndGet(value.size());
		if (old == null) {
			return null;
		} else {
			memoryUsed.addAndGet(-1 * old.getValue().size());
			return old.getAndSetValue(value);
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public CacheElement putIfAbsent(Key key, CacheElement value) {
		if (value == null) {
			throw new IllegalArgumentException();
		}
		Node old = putIfAbsent(new Node(key, value, sentinel, lock));
		if (old == null) {
			memoryUsed.addAndGet(value.size());
			
			return null;
		} else
			return old.getValue();
	}
	
	/**
	 * Adds a node to the list and data store if it does not already exist.
	 * @param node An unlinked node to add.
	 * @return The previous value in the data store.
	 */
	private Node putIfAbsent(Node node) {
		Node old = data.putIfAbsent(node.getKey(), node);
		if (old == null) {
			length.incrementAndGet();
			node.appendToTail();
			evict();
		} else {
			policy.onAccess(this, old);
		}
		return old;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public CacheElement remove(Object key) {
		Node node = data.remove(key);
		if (node == null) {
			return null;
		}
		length.decrementAndGet();
		memoryUsed.addAndGet(-1 * node.getValue().size());
		node.remove();
		
		return node.getValue();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean remove(Object key, Object value) {
		Node node = data.get(key);
		if ((node != null) && node.value.equals(value) && data.remove(key, new Identity(node))) {
			length.decrementAndGet();
			memoryUsed.addAndGet(-1 * node.getValue().size());
			node.remove();
			
			return true;
		}
		return false;
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public CacheElement replace(Key key, CacheElement value) {
		if (value == null) {
			throw new IllegalArgumentException();
		}
		Node node = data.get(key);
		if (node == null)
			return null;
		else {
			memoryUsed.addAndGet(-1 * node.getValue().size());
			memoryUsed.addAndGet(value.size());
			
			return node.getAndSetValue(value);
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean replace(Key key, CacheElement oldValue, CacheElement newValue) {
		if (newValue == null) {
			throw new IllegalArgumentException();
		}
		Node node = data.get(key);
		if (node == null)
			return false;
		else {
			final boolean val = node.casValue(oldValue, newValue);
			if (val) {
				memoryUsed.addAndGet(-1 * oldValue.size());
				memoryUsed.addAndGet(newValue.size());
			}
			return val;
		}
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<Key> keySet() {
		return new KeySet();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Collection<CacheElement> values() {
		return new Values();
	}
	
	/**
	 * {@inheritDoc}
	 */
	@Override
	public Set<Entry<Key, CacheElement>> entrySet() {
		return new EntrySet();
	}
	
	/**
	 * A listener registered for notification when an entry is evicted.
	 */
	public interface EvictionListener {
		
		/**
		 * A call-back notification that the entry was evicted.
		 * @param key The evicted key.
		 * @param value The evicted value.
		 */
		void onEviction(Key key, CacheElement value);
	}
	
	/**
	 * The replacement policy to apply to determine which entry to discard when the capacity has been reached.
	 */
	public enum EvictionPolicy {
		
		/**
		 * Evicts entries based on insertion order.
		 */
		FIFO() {
			@Override
			void onAccess(ConcurrentLinkedHashMap map, Node node) {
				// do nothing
			}
			
			@Override
			boolean onEvict(ConcurrentLinkedHashMap map, Node node) {
				return true;
			}
		},
		
		/**
		 * Evicts entries based on insertion order, but gives an entry a "second chance" if it has been requested recently.
		 */
		SECOND_CHANCE() {
			@Override
			void onAccess(ConcurrentLinkedHashMap map, Node node) {
				node.setMarked(true);
			}
			
			@Override
			boolean onEvict(ConcurrentLinkedHashMap map, Node node) {
				if (node.isMarked()) {
					node.moveToTail();
					node.setMarked(false);
					return false;
				}
				return true;
			}
		},
		
		/**
		 * Evicts entries based on how recently they are used, with the least recent evicted first.
		 */
		LRU() {
			@Override
			void onAccess(ConcurrentLinkedHashMap map, Node node) {
				node.moveToTail();
			}
			
			@Override
			boolean onEvict(ConcurrentLinkedHashMap map, Node node) {
				return true;
			}
		};
		
		/**
		 * Performs any operations required by the policy after a node was successfully retrieved.
		 */
		abstract void onAccess(ConcurrentLinkedHashMap map, Node node);
		
		/**
		 * Determines whether to evict the node at the head of the list.
		 */
		abstract boolean onEvict(ConcurrentLinkedHashMap map, Node node);
	}
	
	/**
	 * A node on the double-linked list. This list cross-cuts the data store.
	 */
	protected static final class Node implements Serializable {
		private static final long serialVersionUID = 1461281468985304519L;
		private static final AtomicReferenceFieldUpdater<Node, CacheElement> valueUpdater = AtomicReferenceFieldUpdater.newUpdater(Node.class, CacheElement.class, "value");
		private static final Node UNLINKED = new Node(null);
		
		private final Key key;
		private final Lock lock;
		private final Node sentinel;
		
		private volatile CacheElement value;
		private volatile boolean marked;
		private volatile Node prev;
		private volatile Node next;
		
		/**
		 * Creates a new sentinel node.
		 */
		public Node(Lock lock) {
			this.sentinel = this;
			this.value = null;
			this.lock = lock;
			this.prev = this;
			this.next = this;
			this.key = null;
		}
		
		/**
		 * Creates a new, unlinked node.
		 */
		public Node(Key key, CacheElement value, Node sentinel, Lock lock) {
			this.sentinel = sentinel;
			this.next = UNLINKED;
			this.prev = UNLINKED;
			this.value = value;
			this.lock = lock;
			this.key = key;
		}
		
		/**
		 * Appends the node to the tail of the list.
		 */
		public void appendToTail() {
			lock.lock();
			try {
				// Allow moveToTail() to no-op or removal to spin-wait
				next = sentinel;
				
				// Read the tail on the stack to avoid unnecessary volatile reads
				final Node tail = sentinel.prev;
				sentinel.prev = this;
				tail.next = this;
				prev = tail;
			} finally {
				lock.unlock();
			}
		}
		
		/**
		 * Removes the node from the list.
		 * <p/>
		 * If the node has not yet been appended to the tail it will wait for that operation to complete.
		 */
		public void remove() {
			for (;;) {
				if (isUnlinked()) {
					continue; // await appending
				}
				
				lock.lock();
				try {
					if (isUnlinked()) {
						continue; // await appending
					}
					prev.next = next;
					next.prev = prev;
					next = UNLINKED; // mark as unlinked
				} finally {
					lock.unlock();
				}
				return;
			}
		}
		
		/**
		 * Moves the node to the tail.
		 * <p/>
		 * If the node has been unlinked or is already at the tail, no-ops.
		 */
		public void moveToTail() {
			if (isTail() || isUnlinked()) {
				return;
			}
			lock.lock();
			try {
				if (isTail() || isUnlinked()) {
					return;
				}
				// unlink
				prev.next = next;
				next.prev = prev;
				
				// link
				next = sentinel; // ordered for isAtTail()
				prev = sentinel.prev;
				sentinel.prev = this;
				prev.next = this;
			} finally {
				lock.unlock();
			}
		}
		
		/**
		 * Checks whether the node is linked on the list chain.
		 * @return Whether the node has not yet been linked on the list.
		 */
		public boolean isUnlinked() {
			return (next == UNLINKED);
		}
		
		/**
		 * Checks whether the node is the last linked on the list chain.
		 * @return Whether the node is at the tail of the list.
		 */
		public boolean isTail() {
			return (next == sentinel);
		}
		
		/*
		 * Key operators
		 */
		public Key getKey() {
			return key;
		}
		
		/*
		 * Value operators
		 */
		public CacheElement getValue() {
			return valueUpdater.get(this);
		}
		
		public CacheElement getAndSetValue(CacheElement value) {
			return valueUpdater.getAndSet(this, value);
		}
		
		public boolean casValue(CacheElement expect, CacheElement update) {
			return valueUpdater.compareAndSet(this, expect, update);
		}
		
		/*
		 * Previous node operators
		 */
		public Node getPrev() {
			return prev;
		}
		
		/*
		 * Next node operators
		 */
		public Node getNext() {
			return next;
		}
		
		/*
		 * Access frequency operators
		 */
		public boolean isMarked() {
			return marked;
		}
		
		public void setMarked(boolean marked) {
			this.marked = marked;
		}
		
		/**
		 * Only ensures that the values are equal, as the key may be <tt>null</tt> for look-ups.
		 */
		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			} else if (!(obj instanceof Node)) {
				return false;
			}
			CacheElement value = getValue();
			Node node = (Node) obj;
			return (value == null) ? (node.getValue() == null) : value.equals(node.getValue());
		}
		
		@Override
		public int hashCode() {
			return ((key == null) ? 0 : key.hashCode()) ^ ((value == null) ? 0 : value.hashCode());
		}
		
		@Override
		public String toString() {
			lock.lock();
			try {
				return String.format("key=%s - prev=%s ; next=%s", valueOf(key), valueOf(prev.key), valueOf(next.key));
			} finally {
				lock.unlock();
			}
		}
		
		private String valueOf(Key key) {
			return (key == null) ? "sentinel" : key.toString();
		}
	}
	
	/**
	 * Allows {@link #equals(Object)} to compare using object identity.
	 */
	private static final class Identity {
		private final Object delegate;
		
		public Identity(Object delegate) {
			this.delegate = delegate;
		}
		
		@Override
		public boolean equals(Object o) {
			return (o == delegate);
		}
	}
	
	/**
	 * An adapter to safely externalize the keys.
	 */
	private final class KeySet extends AbstractSet<Key> {
		private final ConcurrentLinkedHashMap map = ConcurrentLinkedHashMap.this;
		
		@Override
		public int size() {
			return map.size();
		}
		
		@Override
		public void clear() {
			map.clear();
		}
		
		@Override
		public Iterator<Key> iterator() {
			return new KeyIterator();
		}
		
		@Override
		public boolean contains(Object obj) {
			return map.containsKey(obj);
		}
		
		@Override
		public boolean remove(Object obj) {
			return (map.remove(obj) != null);
		}
		
		@Override
		public Object[] toArray() {
			return map.data.keySet().toArray();
		}
		
		@Override
		public <T> T[] toArray(T[] array) {
			return map.data.keySet().toArray(array);
		}
	}
	
	/**
	 * An adapter to safely externalize the keys.
	 */
	private final class KeyIterator implements Iterator<Key> {
		private final EntryIterator iterator = new EntryIterator(ConcurrentLinkedHashMap.this.data.values().iterator());
		
		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}
		
		@Override
		public Key next() {
			return iterator.next().getKey();
		}
		
		@Override
		public void remove() {
			iterator.remove();
		}
	}
	
	/**
	 * An adapter to represent the data store's values in the external type.
	 */
	private final class Values extends AbstractCollection<CacheElement> {
		private final ConcurrentLinkedHashMap map = ConcurrentLinkedHashMap.this;
		
		@Override
		public int size() {
			return map.size();
		}
		
		@Override
		public void clear() {
			map.clear();
		}
		
		@Override
		public Iterator<CacheElement> iterator() {
			return new ValueIterator();
		}
		
		@Override
		public boolean contains(Object o) {
			return map.containsValue(o);
		}
		
		@Override
		public Object[] toArray() {
			Collection<CacheElement> values = new ArrayList<CacheElement>(size());
			for (CacheElement value : this) {
				values.add(value);
			}
			return values.toArray();
		}
		
		@Override
		public <T> T[] toArray(T[] array) {
			Collection<CacheElement> values = new ArrayList<CacheElement>(size());
			for (CacheElement value : this) {
				values.add(value);
			}
			return values.toArray(array);
		}
	}
	
	/**
	 * An adapter to represent the data store's values in the external type.
	 */
	private final class ValueIterator implements Iterator<CacheElement> {
		private final EntryIterator iterator = new EntryIterator(ConcurrentLinkedHashMap.this.data.values().iterator());
		
		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}
		
		@Override
		public CacheElement next() {
			return iterator.next().getValue();
		}
		
		@Override
		public void remove() {
			iterator.remove();
		}
	}
	
	/**
	 * An adapter to represent the data store's entry set in the external type.
	 */
	private final class EntrySet extends AbstractSet<Entry<Key, CacheElement>> {
		private final ConcurrentLinkedHashMap map = ConcurrentLinkedHashMap.this;
		
		@Override
		public int size() {
			return map.size();
		}
		
		@Override
		public void clear() {
			map.clear();
		}
		
		@Override
		public Iterator<Entry<Key, CacheElement>> iterator() {
			return new EntryIterator(map.data.values().iterator());
		}
		
		@Override
		public boolean contains(Object obj) {
			if (!(obj instanceof Entry)) {
				return false;
			}
			Entry<?, ?> entry = (Entry<?, ?>) obj;
			Node node = map.data.get(entry.getKey());
			return (node != null) && (node.value.equals(entry.getValue()));
		}
		
		@Override
		public boolean add(Entry<Key, CacheElement> entry) {
			return (map.putIfAbsent(entry.getKey(), entry.getValue()) == null);
		}
		
		@Override
		public boolean remove(Object obj) {
			if (!(obj instanceof Entry)) {
				return false;
			}
			Entry<?, ?> entry = (Entry<?, ?>) obj;
			return map.remove(entry.getKey(), entry.getValue());
		}
		
		@Override
		public Object[] toArray() {
			Collection<Entry<Key, CacheElement>> entries = new ArrayList<Entry<Key, CacheElement>>(size());
			for (Entry<Key, CacheElement> entry : this) {
				entries.add(new SimpleEntry(entry));
			}
			return entries.toArray();
		}
		
		@Override
		public <T> T[] toArray(T[] array) {
			Collection<Entry<Key, CacheElement>> entries = new ArrayList<Entry<Key, CacheElement>>(size());
			for (Entry<Key, CacheElement> entry : this) {
				entries.add(new SimpleEntry(entry));
			}
			return entries.toArray(array);
		}
	}
	
	/**
	 * An adapter to represent the data store's entry iterator in the external type.
	 */
	private final class EntryIterator implements Iterator<Entry<Key, CacheElement>> {
		private final Iterator<Node> iterator;
		private Entry<Key, CacheElement> current;
		
		public EntryIterator(Iterator<Node> iterator) {
			this.iterator = iterator;
		}
		
		@Override
		public boolean hasNext() {
			return iterator.hasNext();
		}
		
		@Override
		public Entry<Key, CacheElement> next() {
			current = new NodeEntry(iterator.next());
			return current;
		}
		
		@Override
		public void remove() {
			if (current == null) {
				throw new IllegalStateException();
			}
			ConcurrentLinkedHashMap.this.remove(current.getKey(), current.getValue());
			current = null;
		}
	}
	
	/**
	 * An entry that is tied to the map instance to allow updates through the entry or the map to be visible.
	 */
	private final class NodeEntry implements Entry<Key, CacheElement> {
		private final ConcurrentLinkedHashMap map = ConcurrentLinkedHashMap.this;
		private final Node node;
		
		public NodeEntry(Node node) {
			this.node = node;
		}
		
		@Override
		public Key getKey() {
			return node.getKey();
		}
		
		@Override
		public CacheElement getValue() {
			if (node.isUnlinked()) {
				CacheElement value = map.get(getKey());
				if (value != null) {
					return value;
				}
			}
			return node.getValue();
		}
		
		@Override
		public CacheElement setValue(CacheElement value) {
			return map.replace(getKey(), value);
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			} else if (!(obj instanceof Entry)) {
				return false;
			}
			Entry<?, ?> entry = (Entry<?, ?>) obj;
			return eq(getKey(), entry.getKey()) && eq(getValue(), entry.getValue());
		}
		
		@Override
		public int hashCode() {
			Key key = getKey();
			CacheElement value = getValue();
			return ((key == null) ? 0 : key.hashCode()) ^ ((value == null) ? 0 : value.hashCode());
		}
		
		@Override
		public String toString() {
			return getKey() + "=" + getValue();
		}
		
		private boolean eq(Object o1, Object o2) {
			return (o1 == null) ? (o2 == null) : o1.equals(o2);
		}
	}
	
	/**
	 * This duplicates {@link java.util.AbstractMap.SimpleEntry} until the class is made accessible (public in JDK-6).
	 */
	private static class SimpleEntry implements Entry<Key, CacheElement> {
		private final Key key;
		private CacheElement value;
		
		public SimpleEntry(Entry<Key, CacheElement> e) {
			this.key = e.getKey();
			this.value = e.getValue();
		}
		
		@Override
		public Key getKey() {
			return key;
		}
		
		@Override
		public CacheElement getValue() {
			return value;
		}
		
		@Override
		public CacheElement setValue(CacheElement value) {
			CacheElement oldValue = this.value;
			this.value = value;
			return oldValue;
		}
		
		@Override
		public boolean equals(Object obj) {
			if (obj == this) {
				return true;
			} else if (!(obj instanceof Entry)) {
				return false;
			}
			Entry<?, ?> entry = (Entry<?, ?>) obj;
			return eq(key, entry.getKey()) && eq(value, entry.getValue());
		}
		
		@Override
		public int hashCode() {
			return ((key == null) ? 0 : key.hashCode()) ^ ((value == null) ? 0 : value.hashCode());
		}
		
		@Override
		public String toString() {
			return key + "=" + value;
		}
		
		private static boolean eq(Object o1, Object o2) {
			return (o1 == null) ? (o2 == null) : o1.equals(o2);
		}
	}
}

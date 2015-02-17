package com.thimbleware.jmemcached;

import java.io.Serializable;

import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;

import com.thimbleware.jmemcached.util.BufferUtils;

/**
 */
public final class CacheElement implements Serializable {
	
	public final static long THIRTY_DAYS = 2592000000L;
	
	private static final long serialVersionUID = 1L;
	private long expire;
	private int flags;
	private ChannelBuffer data;
	private Key key;
	private long casUnique = 0L;
	private boolean blocked = false;
	private long blockedUntil;
	
	public CacheElement() {
	}
	
	public CacheElement(Key key) {
		this.key = key;
	}
	
	public CacheElement(Key key, int flags, long expire, long casUnique) {
		this.key = key;
		this.flags = flags;
		this.expire = expire;
		this.casUnique = casUnique;
	}
	
	/**
	 * @return the current time in seconds
	 */
	public static int Now() {
		return (int) (System.currentTimeMillis() / 1000);
	}
	
	public int size() {
		return getData().capacity();
	}
	
	public CacheElement append(CacheElement appendElement) {
		int newLength = size() + appendElement.size();
		CacheElement appendedElement = new CacheElement(getKey(), getFlags(), getExpire(), 0L);
		ChannelBuffer appended = ChannelBuffers.buffer(newLength);
		ChannelBuffer existing = getData();
		ChannelBuffer append = appendElement.getData();
		
		appended.writeBytes(existing);
		appended.writeBytes(append);
		
		appended.readerIndex(0);
		
		existing.readerIndex(0);
		append.readerIndex(0);
		
		appendedElement.setData(appended);
		appendedElement.setCasUnique(appendedElement.getCasUnique() + 1);
		
		return appendedElement;
	}
	
	public CacheElement prepend(CacheElement prependElement) {
		int newLength = size() + prependElement.size();
		
		CacheElement prependedElement = new CacheElement(getKey(), getFlags(), getExpire(), 0L);
		ChannelBuffer prepended = ChannelBuffers.buffer(newLength);
		ChannelBuffer prepend = prependElement.getData();
		ChannelBuffer existing = getData();
		
		prepended.writeBytes(prepend);
		prepended.writeBytes(existing);
		
		existing.readerIndex(0);
		prepend.readerIndex(0);
		
		prepended.readerIndex(0);
		
		prependedElement.setData(prepended);
		prependedElement.setCasUnique(prependedElement.getCasUnique() + 1);
		
		return prependedElement;
	}
	
	public static class IncrDecrResult {
		int oldValue;
		CacheElement replace;
		
		public IncrDecrResult(int oldValue, CacheElement replace) {
			this.oldValue = oldValue;
			this.replace = replace;
		}
	}
	
	public IncrDecrResult add(int mod) {
		// T O D O handle parse failure!
		int modVal = BufferUtils.atoi(getData()) + mod; // change value
		if (modVal < 0) {
			modVal = 0;
			
		} // check for underflow
		
		ChannelBuffer newData = BufferUtils.itoa(modVal);
		
		CacheElement replace = new CacheElement(getKey(), getFlags(), getExpire(), 0L);
		replace.setData(newData);
		replace.setCasUnique(replace.getCasUnique() + 1);
		
		return new IncrDecrResult(modVal, replace);
	}
	
	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		
		CacheElement that = (CacheElement) o;
		
		if (blocked != that.blocked) return false;
		if (blockedUntil != that.blockedUntil) return false;
		if (casUnique != that.casUnique) return false;
		if (expire != that.expire) return false;
		if (flags != that.flags) return false;
		if (data != null ? !data.equals(that.data) : that.data != null) return false;
		if (key != null ? !key.equals(that.key) : that.key != null) return false;
		
		return true;
	}
	
	@Override
	public int hashCode() {
		int result = (int) (expire ^ (expire >>> 32));
		result = 31 * result + flags;
		result = 31 * result + (data != null ? data.hashCode() : 0);
		result = 31 * result + (key != null ? key.hashCode() : 0);
		result = 31 * result + (int) (casUnique ^ (casUnique >>> 32));
		result = 31 * result + (blocked ? 1 : 0);
		result = 31 * result + (int) (blockedUntil ^ (blockedUntil >>> 32));
		return result;
	}
	
	public static CacheElement key(Key key) {
		return new CacheElement(key);
	}
	
	public long getExpire() {
		return expire;
	}
	
	public int getFlags() {
		return flags;
	}
	
	public ChannelBuffer getData() {
		data.readerIndex(0);
		return data;
	}
	
	public Key getKey() {
		return key;
	}
	
	public long getCasUnique() {
		return casUnique;
	}
	
	public boolean isBlocked() {
		return blocked;
	}
	
	public long getBlockedUntil() {
		return blockedUntil;
	}
	
	public void setCasUnique(long casUnique) {
		this.casUnique = casUnique;
	}
	
	public void block(long blockedUntil) {
		this.blocked = true;
		this.blockedUntil = blockedUntil;
	}
	
	public void setData(ChannelBuffer data) {
		data.readerIndex(0);
		this.data = data;
	}
	
	public static CacheElement readFromBuffer(ChannelBuffer in) {
		/*int bufferSize =*/in.readInt();
		long expiry = in.readLong();
		int keyLength = in.readInt();
		ChannelBuffer key = in.slice(in.readerIndex(), keyLength);
		in.skipBytes(keyLength);
		CacheElement CacheElement = new CacheElement(new Key(key));
		
		CacheElement.expire = expiry;
		CacheElement.flags = in.readInt();
		
		int dataLength = in.readInt();
		CacheElement.data = in.slice(in.readerIndex(), dataLength);
		in.skipBytes(dataLength);
		
		CacheElement.casUnique = in.readInt();
		CacheElement.blocked = in.readByte() == 1;
		CacheElement.blockedUntil = in.readLong();
		
		return CacheElement;
	}
	
	public int bufferSize() {
		return 4 + 8 + 4 + key.bytes.capacity() + 4 + 4 + 4 + data.capacity() + 8 + 1 + 8;
	}
	
	public void writeToBuffer(ChannelBuffer out) {
		out.writeInt(bufferSize());
		out.writeLong(expire);
		out.writeInt(key.bytes.capacity());
		out.writeBytes(key.bytes);
		out.writeInt(flags);
		out.writeInt(data.capacity());
		out.writeBytes(data);
		out.writeLong(casUnique);
		out.writeByte(blocked ? 1 : 0);
		out.writeLong(blockedUntil);
	}
	
}

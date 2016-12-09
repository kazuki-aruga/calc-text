/**
 * 
 */
package io.github.kazuki_aruga.calc_text;

import java.io.Serializable;

/**
 * @author k-aruga
 *
 */
public class Vocab implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private int vocabId;

	private String proto;

	private String pos;

	private boolean available;

	/**
	 * @return the vocabId
	 */
	public int getVocabId() {
		return vocabId;
	}

	/**
	 * @param vocabId
	 *            the vocabId to set
	 */
	public void setVocabId(int vocabId) {
		this.vocabId = vocabId;
	}

	/**
	 * @return the proto
	 */
	public String getProto() {
		return proto;
	}

	/**
	 * @param proto
	 *            the proto to set
	 */
	public void setProto(String proto) {
		this.proto = proto;
	}

	/**
	 * @return the pos
	 */
	public String getPos() {
		return pos;
	}

	/**
	 * @param pos
	 *            the pos to set
	 */
	public void setPos(String pos) {
		this.pos = pos;
	}

	/**
	 * @return the available
	 */
	public boolean isAvailable() {
		return available;
	}

	/**
	 * @param available
	 *            the available to set
	 */
	public void setAvailable(boolean available) {
		this.available = available;
	}

}

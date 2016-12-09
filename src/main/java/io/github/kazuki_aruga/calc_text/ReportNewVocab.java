/**
 * 
 */
package io.github.kazuki_aruga.calc_text;

import java.io.Serializable;

/**
 * @author k-aruga
 *
 */
public class ReportNewVocab implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String compCode;

	private int year;

	private int vocabId;

	private int wc;

	/**
	 * @return the compCode
	 */
	public String getCompCode() {
		return compCode;
	}

	/**
	 * @param compCode
	 *            the compCode to set
	 */
	public void setCompCode(String compCode) {
		this.compCode = compCode;
	}

	/**
	 * @return the year
	 */
	public int getYear() {
		return year;
	}

	/**
	 * @param year
	 *            the year to set
	 */
	public void setYear(int year) {
		this.year = year;
	}

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
	 * @return the wc
	 */
	public int getWc() {
		return wc;
	}

	/**
	 * @param wc
	 *            the wc to set
	 */
	public void setWc(int wc) {
		this.wc = wc;
	}

}

/**
 * 
 */
package io.github.kazuki_aruga.calc_text;

import java.io.Serializable;

/**
 * @author k-aruga
 *
 */
public class Comp implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String compCode;

	private String compName;

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
	 * @return the compName
	 */
	public String getCompName() {
		return compName;
	}

	/**
	 * @param compName
	 *            the compName to set
	 */
	public void setCompName(String compName) {
		this.compName = compName;
	}

}

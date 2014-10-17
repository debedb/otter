package com.enremmeta.otter;

public class Utils {

	public static String camelCase(String underscored) {
		String lower = underscored.toLowerCase();
		String[] elts = lower.split("_");
		if (elts.length == 0) {
			return capitalize(lower);
		}
		String retval = elts[0];
		for (int i = 1; i < elts.length; i++) {
			retval += capitalize(elts[i]);
		}
		return retval;
	}

	public static String capitalize(String s) {
		return Character.toUpperCase(s.charAt(0)) + s.substring(1);
	}

	public static String underscore(String camelCase) {
		String retval = "";
		for (int i = 0; i < camelCase.length(); i++) {
			char c = camelCase.charAt(i);
			if (Character.isUpperCase(c)) {
				if (retval.length() > 0) {
					retval += "_";
				}
				retval += Character.toLowerCase(c);
			} else {
				retval += c;
			}
		}
		return retval;
	}

}

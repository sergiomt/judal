package org.judal.jdbc.postgresql;

/**
 * © Copyright 2016 the original author.
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

public class Posix {

	  // ----------------------------------------------------------

	  /**
	   * Replace any vowel by a POSIX Regular Expression representing all its accentuated variants
	   * @return If Input String is Andrés Lozäno
	   * the returned value will be something like [AÁÀÄÂAÅAAAÃ]ndr[eéàëêeeeee]s L[oóòöôoooøõo]z[aáàäâaåaaaã]n[oóòöôoooøõo]
	   */
	  public static String accentsToRegEx(String sText) {
	    String[] aSets = new String[]{"aáàäâaåaaaã","eéèëêeeeee","iíìïîiiiiii","oóòöôoooøõō","uúùüûuuuuuuuuu","yýyÿy"};
	    if (null==sText) return null;
	    final int nSets = aSets.length;
	    final int lText = sText.length();
	    final String sLext = sText.toLowerCase();
	    StringBuffer oText = new StringBuffer();
	    for (int n=0; n<lText; n++) {
	      char c = sLext.charAt(n);
	      int iMatch = -1;
	      for (int s=0; s<nSets && -1==iMatch; s++) {
	        if (aSets[s].indexOf(c)>=0) iMatch=s;
	      } // next(s)
	      
	      if (iMatch!=-1)
	      	oText.append("["+(sText.charAt(n)==c ? aSets[iMatch] : aSets[iMatch].toUpperCase())+"]");
	      else
	      	oText.append(sText.charAt(n));
	    } // next (n)
	    return oText.toString();
	  } // AccentsToRegEx

}

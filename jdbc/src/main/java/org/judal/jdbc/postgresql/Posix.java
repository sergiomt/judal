package org.judal.jdbc.postgresql;

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

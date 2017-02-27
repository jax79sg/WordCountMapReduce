package util;

/**
 * Utility class for common operations
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */
public final class CustomProperties {
	public static String keySeparatorsStr="**";
	public static String keySeparatorsRegex="\\*\\*";
	public static String valueSeparatorsRegex="\\t";
	public static String valueSeparatorsStr="\t";
	public static boolean printDebug=false;			//Set this to true if want to print debug messages.
	
	public static void printDebug(String message)
	{
		if (CustomProperties.printDebug){
		System.out.println(message);	}
	}
	
}

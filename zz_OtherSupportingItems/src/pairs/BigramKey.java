package pairs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import util.CustomProperties;

/**
 * Key class for the bigrams
 * This is compatible with Hadoop's WritableComparable and can be transfered to Reducer.
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */

public class BigramKey implements WritableComparable<BigramKey> {

	Text word; //First word
	Text wordPrime;	//subsequent word
	String keySeparatorsStr=CustomProperties.keySeparatorsStr;
	String keySeparatorsRegex=CustomProperties.keySeparatorsRegex;
	
	
	public BigramKey()
	{
		this.word=new Text("");
		this.wordPrime=new Text("");
	}
	
	public BigramKey(String _word,String _wordprime)
	{
		this.word=new Text(_word);
		this.wordPrime=new Text(_wordprime);
	}
	
	/**
	 * I am using Hadoop Text objects to transfer between jobs, so will print text.
	 * string format is W**W'
	 * @return
	 */
	public String generateKeyString()
	{	
		return (word.toString()+keySeparatorsStr+wordPrime.toString());
	}
	
	@Override
	public void readFields(DataInput readin) throws IOException {
		word.readFields(readin);
		wordPrime.readFields(readin);

	}

	@Override
	public void write(DataOutput writeout) throws IOException {
		word.write(writeout);
		wordPrime.write(writeout);
	}

	
	public String getWord() {
		return word.toString();
	}

	public void setWord(String word) {
		this.word = new Text(word);
	}

	public String getWordPrime() {
		return wordPrime.toString();
	}

	public void setWordPrime(String wordPrime) {
		this.wordPrime = new Text(wordPrime);
	}
	
	public String toString(){
		return this.generateKeyString();
	}


	@Override
	public int compareTo(BigramKey o) {		
		return this.generateKeyString().compareTo(o.generateKeyString());
	}
				
				

}

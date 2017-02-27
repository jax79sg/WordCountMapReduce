package pairs;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import util.CustomProperties;

/**
 * Key class for Secondary Sorting technique to let Hadoop framework execution handle top 10 sorting.
 * @author Tan Kah Siong <ucabks0@ucl.ac.uk>
 *
 */

public class CompositeBigramKey implements WritableComparable<CompositeBigramKey> {

	Text word;
	Text wordPrime;
	Text prob;
	String keySeparatorsStr=CustomProperties.keySeparatorsStr;
	String keySeparatorsRegex=CustomProperties.keySeparatorsRegex;
	String valueSeparatorStr=CustomProperties.valueSeparatorsStr;
	
	public CompositeBigramKey()
	{
		this.word=new Text("");
		this.wordPrime=new Text("");
		this.prob=new Text("");
	}
	
	public CompositeBigramKey(String _key,String prob)
	{	
		//_key pattern is w**w'
		_key.split(keySeparatorsRegex);
		this.word=new Text(_key.split(keySeparatorsRegex)[0]);
		this.wordPrime=new Text(_key.split(keySeparatorsRegex)[1]);
		this.prob=new Text(prob);
	}
	
	public String generateKeyString()
	{
		//Pattern is	w**w' 	prob
		return (word.toString()+keySeparatorsStr+wordPrime.toString()+valueSeparatorStr+prob);
	}
	
	@Override
	public void readFields(DataInput readin) throws IOException {
		word.readFields(readin);
		wordPrime.readFields(readin);
		prob.readFields(readin);

	}

	@Override
	public void write(DataOutput writeout) throws IOException {
		word.write(writeout);
		wordPrime.write(writeout);
		prob.write(writeout);

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
	
	public String getProb() {
		return prob.toString();
	}

	public void setProb(String prob) {
		this.prob = new Text(prob);
	}	
	
	public String toString(){
		return this.generateKeyString();
	}

	/**
	 * Hadoop framework will use this class during the sorting before it reaches reducers.
	 * This method will be called, this is where i manipulate the sorting such that the probability is included for the sorting, in desc order.
	 * 
	 */
	@Override
	public int compareTo(CompositeBigramKey o) {		
		int compareResult = this.getWord().compareTo(o.getWord());
		if (compareResult==0)
		{	Float thisProb=new Float(this.getProb());
			Float oProb=new Float(o.getProb());
			compareResult = thisProb.compareTo(oProb);
			if(compareResult==0)
			{
				compareResult = this.getWordPrime().compareTo(o.getWordPrime());
			}
			else
			{
				compareResult = compareResult*-1;	//Return descending order for prob
			}
		}
		return compareResult; 
	}

}

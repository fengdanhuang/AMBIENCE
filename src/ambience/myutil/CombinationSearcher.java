package ambience.myutil;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
*Used for processing the combinations of geno and pheno varaibles.
*@author:  Linhui Tang, Chao Feng
*/

public class CombinationSearcher {

	public static Map<Integer, Integer> search(List<Integer> col_comb, String line) {
		Map<Integer, Integer> combinations = new HashMap<Integer, Integer>();
		String[] values = line.split("\t");
		for (Integer i : col_comb) {
			String value = values[i - 1];
			combinations.put(i, Integer.valueOf(value.trim()));
		}
		return combinations;
	}

	public static String search2(List<Integer> col_comb, String line) {
		String[] values = line.split("\t");
		StringBuilder sb = new StringBuilder();
		for (Integer i : col_comb) {
//			System.out.println("i="+i);
			String value = values[i - 1];
			if (value.trim().equals("-99")) {
				return "";
			}
			sb.append(Integer.valueOf(value.trim()));
			sb.append(",");
		}
		return sb.toString().substring(0, sb.lastIndexOf(","));
	}
	
	public static HashMap<String,String> searchAndReturnHash(List<Integer> col_comb, String line) {
		
		//Process the single element subset.
		String[] values = line.split("\t");
		HashMap <String,String>colval = new HashMap<String, String>(); 
		for (Integer i : col_comb) {
			String value = values[i - 1];
			if (value.trim().equals("-99")) {
				return null;
			}
			colval.put(i.toString(), value.trim());
//			System.out.println("Single:"+i.toString()+"||"+value.trim());
		}
		
		//Process the at least two element subset.
		String col_comb_str = Tools.listToString(col_comb);
		List <String>colcom_subSet = Tools.getSubset(col_comb_str);
		
		for (String atLeastTwoColumn:colcom_subSet){
			StringBuilder atLeastTwoValue = new StringBuilder();//used for collecting value for each combination.
			if(atLeastTwoColumn.contains(",")){//this means that the subcolumn containing at least two elements.
				String[] subColumn = atLeastTwoColumn.split(",");
				int i = 0;
				while (i < subColumn.length) {
					String subValue = colval.get(subColumn[i]);
					atLeastTwoValue.append(subValue.trim());
					atLeastTwoValue.append(",");
					i++;
				}
				colval.put(atLeastTwoColumn, atLeastTwoValue.toString().substring(0, atLeastTwoValue.lastIndexOf(",")));
//				System.out.println("At Least Two:"+atLeastTwoColumn+"||"+ atLeastTwoValue.toString().substring(0, atLeastTwoValue.lastIndexOf(",")));
			}
		}
		
		//return all the subset column and their corresponding values.
		return colval;
	}
	
	
	public static List<Integer> formCombination(String col_comb,int current_column) {
		List<Integer> col_combnation = new LinkedList<Integer>();
		String[] columns = col_comb.split(",");
		int i = 0;
		while (i < columns.length) {
			if (current_column == Integer.parseInt(columns[i])) {
				return null;
			}
			col_combnation.add(Integer.parseInt(columns[i++]));
		}
		col_combnation.add(current_column);
		return col_combnation;
	}

	public static List<Integer> getDuplicateKey(String comb,List<String> preCombs) {

		String[] character = comb.split(",");
		int length = character.length;
		List<Integer> duplicatedKeys = new LinkedList<Integer>();

		for (String preComb : preCombs) {
			String[] str_preComb = preComb.split(",");
			Set<String> strset = new HashSet<String>();
			for (int i = 0; i < length; i++) {
				strset.add(str_preComb[i]);
			}
			for (int i = 0; i < length; i++) {
				if (strset.contains(character[i])) {
					strset.remove(character[i]);
				}
			}
			if (strset.size() == 1) {
				duplicatedKeys.add(Integer.valueOf(strset.iterator().next()));
			}
		}
		return duplicatedKeys;
	}

	public static String sortComb(String comb) {

		String elementComb = comb;
		//slit the string comb
		String[] elements = elementComb.split(",");
		int[] elements_int = new int[elements.length];
		for (int i = 0; i < elements.length; i++) {
			//assign all chars(numbers) to a int array 
			elements_int[i] = Integer.valueOf(elements[i]);
		}
		//sort this array,namely make "2,1 3" be "1,2,3"
		Arrays.sort(elements_int);
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < elements.length; i++) {
			//rebuit the string combinations of sorted variables by stringBuilder
			sb.append(String.valueOf(elements_int[i]));
			if (i != elements.length - 1) {
				//each also add a "," to seperate each variable.
				sb.append(",");
			}
		}
		return sb.toString();
	}
}

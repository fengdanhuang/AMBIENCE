package ambience.myutil;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

/**
*Implement basic tools to process combinations.
*@author:  Linhui Tang, Chao Feng
*/

public class Tools {
	//Integer list transfered to string
	public static String listToString(List<Integer> integerList){
		if (integerList==null){
			return null;
		}
		StringBuilder result = new StringBuilder();
		boolean flag=false;
		for (Integer i: integerList){
				if (flag){
					result.append(",");
				}else{
					flag=true;
				}
				result.append(i.toString());
		}
		return result.toString();
	}
	
	//get the subset of the combination.
	public static List<String> getSubset(String comb) {
		List<String> combinations = new ArrayList<String>();
		String[] elements = comb.split(",");

		int nTerms = elements.length;
		int i = 1;
		while (i < nTerms) {
			CombinationGenerator cg = new CombinationGenerator(elements.length,i);
			StringBuffer combination;
			int[] indices;
			while (cg.hasMore()) {
				combination = new StringBuffer();
				indices = cg.getNext();
				for (int j = 0; j < indices.length; j++) {
					if (j != indices.length - 1) {
						combination.append(elements[indices[j]]).append(",");
					} else {
						combination.append(elements[indices[j]]);
					}
				}
				combinations.add(combination.toString());
//				System.out.println(combination.toString());
			}
			i++;
		}
//		System.out.println(combinations);
		return combinations;
	}
	
	public static void main(String[] args){
		
		//ListToString test.
		List<Integer> list = new LinkedList<Integer>();
		list.add(5);
		list.add(3);
		list.add(4);
		System.out.println(list);
		String tem = listToString(list);
		System.out.println(tem);
		
		//getSubset test.
		String comb = "5,2,3";
		Tools.getSubset(comb);	
	}
}

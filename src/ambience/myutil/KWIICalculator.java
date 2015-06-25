package ambience.myutil;

import java.util.ArrayList;
import java.util.List;

/**
*Calculate the KWII(K-way interaction information) value.
*@author:  Chao Feng
*/


public class KWIICalculator {

	public static List<String> getComb(String comb) {
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
		return combinations;
	}

	public static Double lookUp(List<String> PAIs, String comb) {
		Double kwii = 0.0;
		for (String line : PAIs) {
			//find the specific comb(subset) from the lines in the records.
			if (comb.equals(line.split("\t")[0])) {
				kwii = Double.valueOf(line.split("\t")[2]);
				break;
			}					
		}		
		//if we can not find comb(subset) from the record, then kwii=0.0, this will lead to the result kwii(current order kwii) wrong.
		return kwii;
	}

	public static Double calKwii(Double pai, List<Double> kwii_lowerOrder) {
		Double kwii = 0.0;
		//formular: KWII(A,B,C,P)=PAI(A,B,C,P)-KWII(A,P)-KWII(B,P)-KWII(C,P)-KWII(A,B,P)-...(all subset KWII)
		for (Double d : kwii_lowerOrder) {
			pai = pai - d;
		}
		kwii = pai;
		return kwii;
	}

//	public static void main(String[] args) {
//		String comb = "1,2,3,4,5,6";
//		KWIICalculator.getComb(comb);
//	}
}

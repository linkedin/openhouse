package com.linkedin.openhouse.datalayout.layoutselection.columnorder;

import java.util.ArrayList;

/**
 * Brute force column order generator that takes as input all available columns
 * and generates all possible permutations.
 * TODO: Add a version of this with a limited number of columns.
 */
public class BruteForceColumnOrderGenerator implements ColumnOrderGenerator {

    public List<List<String>> generateOrder(Set<String> availableColumns) {
        List<String> columns = new ArrayList<String>();
        columns.addAll(availableColumns);

        return generatePerm(columns);
    }

    private List<List<String>> generatePerm(List<String> original) {
        if (original.isEmpty()) {
            List<List<String>> result = new ArrayList<>();
            result.add(new ArrayList<>());
            return result;
        }
        String firstElement = original.remove(0);
        List<List<String>> returnValue = new ArrayList<>();
        List<List<String>> permutations = generatePerm(original);
        for (List<String> smallerPermutated : permutations) {
            for (int index = 0; index <= smallerPermutated.size(); index++) {
                List<String> temp = new ArrayList<>(smallerPermutated);
                temp.add(index, firstElement);
                returnValue.add(temp);
            }
        }
        return returnValue;
    }
    
}

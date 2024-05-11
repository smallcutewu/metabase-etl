package com.boulderai.metabase.etl.e.api.engine;

import java.util.Arrays;

public class Test {

    public static void main( String [] args) {

    }

    public int getMaxnum ( int [] arr, int k) {
        int result = arr[0];
        for (int i = 0; i< arr.length ; i ++) {
            for (int j = i; j< arr.length ; j++) {
                if (arr[j] < arr[j+1] ) {
                    int swap = arr[j] ;
                    arr[j] = arr[j+1];
                    arr[j+1] = swap;
                }
            }
        }
        result = arr[k];
        return result;
    }


}

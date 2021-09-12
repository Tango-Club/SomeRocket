package io.openmessaging;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;

public class FastScanner {
    BufferedReader br;
    StringTokenizer st;
    FastScanner(FileReader fd) {
        try	{
            br = new BufferedReader(fd);
            st = new StringTokenizer(br.readLine());
        } catch (Exception e){e.printStackTrace();}
    }
    private String next() {
        if (st.hasMoreTokens())	return st.nextToken();
        try {st = new StringTokenizer(br.readLine());}
        catch (Exception e) {e.printStackTrace();}
        return st.nextToken();
    }
    private int nextInt() {return Integer.parseInt(next());}
    private long nextLong() {return Long.parseLong(next());}
    private double nextDouble() {return Double.parseDouble(next());}
    private String nextLine() {
        String line = "";
        if(st.hasMoreTokens()) line = st.nextToken();
        else try {return br.readLine();}catch(IOException e){e.printStackTrace();}
        while(st.hasMoreTokens()) line += " "+st.nextToken();
        return line;
    }
    private int[] nextIntArray(int n) {
        int[] a = new int[n];
        for(int i = 0; i < n; i++) a[i] = nextInt();
        return a;
    }
    private long[] nextLongArray(int n){
        long[] a = new long[n];
        for(int i = 0; i < n; i++) a[i] = nextLong();
        return a;
    }
    private double[] nextDoubleArray(int n){
        double[] a = new double[n];
        for(int i = 0; i < n; i++) a[i] = nextDouble();
        return a;
    }
    private char[][] nextGrid(int n, int m){
        char[][] grid = new char[n][m];
        for(int i = 0; i < n; i++) grid[i] = next().toCharArray();
        return grid;
    }
}
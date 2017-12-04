package sorting;

import java.util.Comparator;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

public class ParallelMergeSorter<E> extends RecursiveAction {
	
	private Integer[] a;
	private Comparator<Integer> comp;
	private int from;
	private int to;
	private int maxThreads;
	private static int availableThreads;
	
	/**
     * Sorts an array, using the merge sort algorithm.
     *
     * @param a the array to sort
     * @param comp the comparator to compare array elements
     */
    @SuppressWarnings("unchecked")
	public ParallelMergeSorter(E[] a, Comparator<? super E> comp, int maxThreads) {
    	if (maxThreads > SortTester.maxThreads) {
    		System.out.println("Requested more threads than CPU cores!");
    		return;
    	}
    	
    	System.out.println("Max Threads (in constructor): " + maxThreads);
    	
    	if (this.maxThreads != maxThreads) {
    		this.maxThreads = maxThreads;
    		this.availableThreads = maxThreads;
    	}
    	this.a = (Integer[])a;
    	this.comp = (Comparator<Integer>)comp;
    }
    
    /**
     * Sorts an array, using the merge sort algorithm.
     *
     * @param a the array to sort
     * @param comp the comparator to compare array elements
     */
    @SuppressWarnings("unchecked")
	public ParallelMergeSorter(E[] a, int from, int to, Comparator<? super E> comp, int maxThreads) {
    	
    	this.a = (Integer[])a;
    	this.comp = (Comparator<Integer>)comp;
    	this.from = from;
    	this.to = to;
    }

    /**
     * Sorts a range of an array, using the merge sort algorithm.
     *
     * @param a the array to sort
     * @param from the first index of the range to sort
     * @param to the last index of the range to sort
     * @param comp the comparator to compare array elements
     */
    private <E> void parallelMergeSort(E[] a, int from, int to,
            Comparator<? super E> comp) {
        if (from == to) {
            return;
        }
        int mid = (from + to) / 2;
        // Sort the first and the second half
        //System.out.println("=============== PARALLEL ===============");
        
        if (availableThreads <= 1) {
        	mergeSort(a, from, mid, comp);
            mergeSort(a, mid + 1, to, comp);
        }
        else {
	        availableThreads -= 2;
	        
	        invokeAll(new ParallelMergeSorter(a, from, mid, comp, maxThreads),
	        		  new ParallelMergeSorter(a, mid, to, comp, maxThreads));
	        
	        availableThreads += 2;
        }
        merge(a, from, mid, to, comp);
    }

    /**
     * Sorts a range of an array, using the merge sort algorithm.
     *
     * @param a the array to sort
     * @param from the first index of the range to sort
     * @param to the last index of the range to sort
     * @param comp the comparator to compare array elements
     */
    private <E> void mergeSort(E[] a, int from, int to,
            Comparator<? super E> comp) {
    	if (from == to) {
            return;
        }
        int mid = (from + to) / 2;
        // Sort the first and the second half
        //System.out.println("=============== SERIAL ===============");

        if (availableThreads <= 1) {
        	mergeSort(a, from, mid, comp);
            mergeSort(a, mid + 1, to, comp);
        }
        else {
	        availableThreads -= 2;
	        
	        invokeAll(new ParallelMergeSorter(a, from, mid, comp, maxThreads),
	        		  new ParallelMergeSorter(a, mid, to, comp, maxThreads));
	        
	        availableThreads += 2;
        }
        merge(a, from, mid, to, comp);
    }
    
    /**
     * Merges two adjacent subranges of an array
     *
     * @param a the array with entries to be merged
     * @param from the index of the first element of the first range
     * @param mid the index of the last element of the first range
     * @param to the index of the last element of the second range
     * @param comp the comparator to compare array elements
     */
    @SuppressWarnings("unchecked")
    private static <E> void merge(E[] a,
            int from, int mid, int to, Comparator<? super E> comp) {
        int n = to - from + 1;
         // Size of the range to be merged

        // Merge both halves into a temporary array b
        Object[] b = new Object[n];

        int i1 = from;
        // Next element to consider in the first range
        int i2 = mid + 1;
        // Next element to consider in the second range
        int j = 0;
         // Next open position in b

        // As long as neither i1 nor i2 past the end, move
        // the smaller element into b
        while (i1 <= mid && i2 <= to) {
            if (comp.compare(a[i1], a[i2]) < 0) {
                b[j] = a[i1];
                i1++;
            } else {
                b[j] = a[i2];
                i2++;
            }
            j++;
        }

        // Note that only one of the two while loops
        // below is executed
        // Copy any remaining entries of the first half
        while (i1 <= mid) {
            b[j] = a[i1];
            i1++;
            j++;
        }

        // Copy any remaining entries of the second half
        while (i2 <= to) {
            b[j] = a[i2];
            i2++;
            j++;
        }

        // Copy back from the temporary array
        for (j = 0; j < n; j++) {
            a[from + j] = (E) b[j];
        }
    }

	@Override
	protected void compute() {
		parallelMergeSort(a, 0, a.length - 1, comp);
	}
    
}

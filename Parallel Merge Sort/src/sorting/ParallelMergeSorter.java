package sorting;

import java.util.Comparator;
import java.util.concurrent.RecursiveAction;

public class ParallelMergeSorter<E> extends RecursiveAction {

	private E[] a;
	private int from;
	private int to;
	private Comparator<? super E> comp;
	private static int availableThreads;
	
	public ParallelMergeSorter(E[] a, Comparator<? super E> comp, int maxThreads) {
		this.a = a;
		this.comp = comp;
		availableThreads = maxThreads;
	}
	
	public ParallelMergeSorter(E[] a, int from, int to, Comparator<? super E> comp) {
		this.a = a;
    	this.comp = comp;
	}
	
	private void parallelMergeSort(E[] a, int from, int to, Comparator<? super E> comp) {
		if (from == to) {
			return;
		}
		int mid = (from + to) / 2;
		
		// Sort the first and the second half
		System.out.println("Available Threads: " + availableThreads);
		if (availableThreads <= 1) {
			serialMergeSort(a, from, mid, comp);
			serialMergeSort(a, mid + 1, to, comp);
		}
		else {
			availableThreads -= 2;
			
			System.out.println("============== PARALLEL ===============");
			
			RecursiveAction leftSort = new ParallelMergeSorter(a, from, mid, comp);
			RecursiveAction rightSort = new ParallelMergeSorter(a, mid + 1, to, comp);
			
			invokeAll(leftSort, rightSort);+
			
			availableThreads += 2;
		}
		
		merge(a, from, mid, to, comp);
	}
	
	private void serialMergeSort(E[] a, int from, int to, Comparator<? super E> comp) {
		if (from == to) {
			return;
		}
		int mid = (from + to) / 2;
		
		// Sort the first and the second half
		if (availableThreads <= 1) {
			serialMergeSort(a, from, mid, comp);
			serialMergeSort(a, mid + 1, to, comp);
		}
		else {
			availableThreads -= 2;
			
			System.out.println("============== PARALLEL ===============");
			
			RecursiveAction leftSort = new ParallelMergeSorter(a, from, mid, comp);
			RecursiveAction rightSort = new ParallelMergeSorter(a, mid + 1, to, comp);
			
			invokeAll(leftSort, rightSort);
			
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
		if (a.length != 1) {
			//availableThreads--;
			parallelMergeSort(a, 0, a.length - 1, comp);
			//availableThreads++;
		}
	}
	
}

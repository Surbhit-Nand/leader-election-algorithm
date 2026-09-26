package com.cs324.worker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import com.cs324.worker.WorkerTaskCalculator.PrimeRange;

/** Splits jobs into balanced chunks and combines the workers' answers. */
public final class WorkerTaskDistributor {
    private WorkerTaskDistributor() {}

    public static List<Object> split(String jobType, Object payload, int workers) {
        if (workers < 1) throw new IllegalArgumentException("At least one worker is required.");
        if (!"MAX".equalsIgnoreCase(jobType) && !"PRIMESUM".equalsIgnoreCase(jobType)
                && !"PRIMECOUNT".equalsIgnoreCase(jobType)) {
            throw new IllegalArgumentException("Unsupported job type: " + jobType);
        }
        List<Object> chunks = new ArrayList<>();
        // Split a range into inclusive sections with no gaps or overlaps. Use long
        // arithmetic so ranges near the integer limits do not overflow.
        if (payload instanceof PrimeRange range) {
            if ("MAX".equalsIgnoreCase(jobType)) {
                throw new IllegalArgumentException("MAX requires a list, not a prime range.");
            }
            long length = (long) range.end() - range.start() + 1;
            int count = (int) Math.min(workers, length);
            long start = range.start();
            for (int i = 0; i < count; i++) {
                long size = length / count + (i < length % count ? 1 : 0);
                chunks.add(new PrimeRange((int) start, (int) (start + size - 1)));
                start += size;
            }
            return chunks;
        }
        // Copy list input into an integer array and reject invalid entries.
        // Even a list of two numbers stays a list, rather than becoming a range.
        int[] values;
        if (payload instanceof int[] input) {
            values = input.clone();
        } else {
            Collection<?> input;
            if (payload instanceof Integer[] array) input = Arrays.asList(array);
            else if (payload instanceof Collection<?> collection) input = collection;
            else throw new IllegalArgumentException("Expected an integer list or PrimeRange.");
            values = input.stream().mapToInt(value -> {
                if (value instanceof Integer integer) return integer;
                throw new IllegalArgumentException("List entries must be non-null Integer values.");
            }).toArray();
        }
        // Empty prime lists produce zero and need only a local task.
        // An empty list has no maximum, so reject it for MAX.
        if (values.length == 0) {
            if ("MAX".equalsIgnoreCase(jobType)) {
                throw new IllegalArgumentException("MAX requires at least one value.");
            }
            chunks.add(values);
            return chunks;
        }
        // Give each worker a similar number of values, with any extra values going
        // to the first chunks. Use fewer workers when the list is small.
        int count = Math.min(workers, values.length);
        int start = 0;
        for (int i = 0; i < count; i++) {
            int size = values.length / count + (i < values.length % count ? 1 : 0);
            chunks.add(Arrays.copyOfRange(values, start, start + size));
            start += size;
        }
        return chunks;
    }

    // Take the largest partial MAX, or add the partial prime sums/counts.
    // Checked arithmetic reports overflow instead of returning a wrong value.
    public static Number combine(String jobType, Collection<Number> results) {
        if ("MAX".equalsIgnoreCase(jobType)) {
            return results.stream().mapToInt(Number::intValue).max().orElseThrow();
        }
        long sum = 0;
        for (Number result : results) sum = Math.addExact(sum, result.longValue());
        if ("PRIMESUM".equalsIgnoreCase(jobType)) return Long.valueOf(sum);
        if ("PRIMECOUNT".equalsIgnoreCase(jobType)) return Integer.valueOf(Math.toIntExact(sum));
        throw new IllegalArgumentException("Unsupported job type: " + jobType);
    }
}

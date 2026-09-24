package com.cs324.worker;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;
import java.util.stream.IntStream;

/** Stateless calculations using the worker's existing payload and result contract. */
public final class WorkerTaskCalculator {
    private WorkerTaskCalculator() {}

    /**
     * Case-insensitive operations; MAX/count return Integer and sum returns Long.
     * Two-element int arrays are inclusive ranges for both prime operations.
     * Other int arrays, Integer arrays and numeric collections are lists.
     * Null/unsupported payloads are empty; null boxed entries and nonnumeric
     * collection entries are ignored. Numbers use intValue(), as in the worker.
     * Empty inputs and reversed ranges return zero.
     */
    public static Number calculate(String jobType, Object payload) {
        if ("MAX".equalsIgnoreCase(jobType)) {
            return numbers(jobType, payload).max().orElse(0);
        }
        if ("PRIMESUM".equalsIgnoreCase(jobType)) {
            return numbers(jobType, payload).filter(WorkerTaskCalculator::isPrime)
                    .asLongStream().reduce(0L, Math::addExact);
        }
        if ("PRIMECOUNT".equalsIgnoreCase(jobType)) {
            return Math.toIntExact(numbers(jobType, payload)
                    .filter(WorkerTaskCalculator::isPrime).count());
        }
        throw new IllegalArgumentException("Unsupported job type: " + jobType);
    }

    private static IntStream numbers(String jobType, Object payload) {
        if (payload instanceof int[] values) {
            if (values.length == 2 && !"MAX".equalsIgnoreCase(jobType)) {
                // Avoid allocating the range or overflowing its upper endpoint.
                return IntStream.rangeClosed(values[0], values[1]);
            }
            return Arrays.stream(values);
        }
        if (payload instanceof Integer[] values) {
            return Arrays.stream(values).filter(Objects::nonNull).mapToInt(Integer::intValue);
        }
        if (payload instanceof Collection<?> values) {
            return values.stream().filter(Number.class::isInstance)
                    .mapToInt(value -> ((Number) value).intValue());
        }
        return IntStream.empty();
    }

    /** Exact primality check without multiplication overflow. */
    public static boolean isPrime(int value) {
        if (value < 2) return false;
        if (value % 2 == 0) return value == 2;
        for (int divisor = 3; divisor <= value / divisor; divisor += 2) {
            if (value % divisor == 0) return false;
        }
        return true;
    }
}

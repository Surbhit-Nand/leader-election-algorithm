package com.cs324.worker;

import java.util.Arrays;
import java.util.Collection;
import java.io.Serializable;
import java.util.stream.IntStream;

/** Shared worker calculations with explicit list and range payloads. */
public final class WorkerTaskCalculator {
    private WorkerTaskCalculator() {}

    /** Inclusive prime range, serializable for distributed worker assignments. */
    public record PrimeRange(int start, int end) implements Serializable {
        public PrimeRange {
            if (start > end) {
                throw new IllegalArgumentException("Range start must not exceed end.");
            }
        }
    }

    /**
     * Case-insensitive operations; MAX/count return Integer and sum returns Long.
     * int arrays, Integer arrays and collections of Integer are always lists.
     * PrimeRange explicitly selects an inclusive range for either prime operation.
     * Invalid payloads/elements and empty MAX lists throw IllegalArgumentException.
     * Empty prime lists return zero. No numeric narrowing conversions are performed.
     */
    public static Number calculate(String jobType, Object payload) {
        // Apply the selected calculation to the input. Prime sums use long values
        // because their total can be larger than a single integer.
        if ("MAX".equalsIgnoreCase(jobType)) {
            return numbers(jobType, payload).max()
                    .orElseThrow(() -> new IllegalArgumentException("MAX requires at least one value."));
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
        if (payload instanceof PrimeRange range) {
            if ("MAX".equalsIgnoreCase(jobType)) {
                throw new IllegalArgumentException("MAX requires a list, not a prime range.");
            }
            // Negative numbers and 0/1 cannot be prime; skip them without iterating.
            return IntStream.rangeClosed(Math.max(2, range.start()), range.end());
        }
        // Read arrays and collections as lists, preserving repeated values.
        // Reject non-integer entries rather than silently changing their values.
        if (payload instanceof int[] values) {
            return Arrays.stream(values);
        }
        if (payload instanceof Integer[] values) {
            return Arrays.stream(values).mapToInt(WorkerTaskCalculator::requireInteger);
        }
        if (payload instanceof Collection<?> values) {
            return values.stream().mapToInt(WorkerTaskCalculator::requireInteger);
        }
        throw new IllegalArgumentException("Payload must be int[], Integer[], a collection of Integer, or PrimeRange.");
    }

    private static int requireInteger(Object value) {
        if (value instanceof Integer integer) {
            return integer;
        }
        throw new IllegalArgumentException("List entries must be non-null Integer values.");
    }

    /** Exact primality check without multiplication overflow. */
    public static boolean isPrime(int value) {
        if (value < 2) {
            return false;
        }
        if (value % 2 == 0) {
            return value == 2;
        }
        // After checking even numbers, try odd divisors up to the square root.
        // Division avoids overflow that could occur when squaring the divisor.
        for (int divisor = 3; divisor <= value / divisor; divisor += 2) {
            if (value % divisor == 0) {
                return false;
            }
        }
        return true;
    }
}

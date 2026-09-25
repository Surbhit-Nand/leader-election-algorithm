package com.cs324.worker;

import java.util.Arrays;
import java.util.List;
import com.cs324.worker.WorkerTaskCalculator.PrimeRange;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class WorkerTaskCalculatorTest {
    @Test
    void smallPrimesAndRangesCrossingZeroAreCalculatedCorrectly() {
        for (int value : new int[]{2, 3, 5, 7, 97}) {
            assertTrue(WorkerTaskCalculator.isPrime(value), "Expected prime: " + value);
        }
        assertEquals(Long.valueOf(5), WorkerTaskCalculator.calculate("PRIMESUM", new PrimeRange(Integer.MIN_VALUE, 3)));
        assertEquals(Integer.valueOf(2), WorkerTaskCalculator.calculate("PRIMECOUNT", new PrimeRange(-5, 3)));
        assertEquals(Long.valueOf(0), WorkerTaskCalculator.calculate("PRIMESUM", new PrimeRange(-5, 1)));
        assertEquals(Integer.valueOf(0), WorkerTaskCalculator.calculate("PRIMECOUNT", new PrimeRange(8, 10)));
    }

    @Test
    void twoElementListChunksNeverBecomeRanges() {
        int[] first = {2, 7};
        int[] second = {5, 5};
        int[] whole = {2, 7, 5, 5};
        assertEquals(Integer.valueOf(2), WorkerTaskCalculator.calculate("PRIMECOUNT", first));
        assertEquals(Long.valueOf(9), WorkerTaskCalculator.calculate("PRIMESUM", first));
        assertEquals(Integer.valueOf(2), WorkerTaskCalculator.calculate("PRIMECOUNT", second));
        for (String operation : new String[]{"PRIMESUM", "PRIMECOUNT"}) {
            assertEquals(WorkerTaskCalculator.calculate(operation, whole).longValue(),
                    WorkerTaskCalculator.calculate(operation, first).longValue()
                            + WorkerTaskCalculator.calculate(operation, second).longValue());
        }
        assertEquals(WorkerTaskCalculator.calculate("MAX", whole).intValue(),
                Math.max(WorkerTaskCalculator.calculate("MAX", first).intValue(),
                        WorkerTaskCalculator.calculate("MAX", second).intValue()));
    }

    @Test
    void separateRangesCombineToTheWholeRange() {
        for (String operation : new String[]{"PRIMESUM", "PRIMECOUNT"}) {
            assertEquals(WorkerTaskCalculator.calculate(operation, new PrimeRange(1, 100)).longValue(),
                    WorkerTaskCalculator.calculate(operation, new PrimeRange(1, 50)).longValue()
                            + WorkerTaskCalculator.calculate(operation, new PrimeRange(51, 100)).longValue());
        }
    }

    @Test
    void maxUsesIntegerListsIncludingMinimumValue() {
        assertEquals(Integer.valueOf(45), WorkerTaskCalculator.calculate("max", new int[]{3, 7, 12, 45}));
        assertEquals(Integer.valueOf(5), WorkerTaskCalculator.calculate("MAX", new int[]{5, 3}));
        assertEquals(Integer.valueOf(Integer.MIN_VALUE),
                WorkerTaskCalculator.calculate("MAX", new int[]{Integer.MIN_VALUE}));
        assertEquals(Integer.valueOf(-3), WorkerTaskCalculator.calculate("MAX", new int[]{-10, -3, -9}));
    }

    @Test
    void explicitRangesIncludeBothEndpoints() {
        assertEquals(Long.valueOf(17), WorkerTaskCalculator.calculate("primesum", new PrimeRange(1, 10)));
        assertEquals(Integer.valueOf(4), WorkerTaskCalculator.calculate("primecount", new PrimeRange(2, 7)));
        assertEquals(Long.valueOf(7), WorkerTaskCalculator.calculate("PRIMESUM", new PrimeRange(7, 7)));
        assertEquals(Integer.valueOf(1), WorkerTaskCalculator.calculate("PRIMECOUNT", new PrimeRange(7, 7)));
        assertThrows(IllegalArgumentException.class, () -> new PrimeRange(10, 1));
        assertThrows(IllegalArgumentException.class, () -> WorkerTaskCalculator.calculate("MAX", new PrimeRange(1, 10)));
    }

    @Test
    void allListTypesAgreeAndRetainDuplicates() {
        for (Object payload : new Object[]{new int[]{2, 7}, new Integer[]{2, 7}, List.of(2, 7)}) {
            assertEquals(Integer.valueOf(2), WorkerTaskCalculator.calculate("PRIMECOUNT", payload));
            assertEquals(Long.valueOf(9), WorkerTaskCalculator.calculate("PRIMESUM", payload));
        }
        assertEquals(Integer.valueOf(4), WorkerTaskCalculator.calculate("PRIMECOUNT", new int[]{2, 3, 4, 5, 5}));
        assertEquals(Long.valueOf(15), WorkerTaskCalculator.calculate("PRIMESUM", new int[]{2, 3, 5, 5}));
    }

    @Test
    void emptyListsHaveNoMaximumButZeroPrimeSumAndCount() {
        for (Object payload : new Object[]{new int[0], new Integer[0], List.of()}) {
            assertThrows(IllegalArgumentException.class, () -> WorkerTaskCalculator.calculate("MAX", payload));
            assertEquals(Integer.valueOf(0), WorkerTaskCalculator.calculate("PRIMECOUNT", payload));
            assertEquals(Long.valueOf(0), WorkerTaskCalculator.calculate("PRIMESUM", payload));
        }
    }

    @Test
    void rejectsInvalidPayloadsAndUnsafeNumberConversions() {
        for (String operation : new String[]{"MAX", "PRIMESUM", "PRIMECOUNT"}) {
            for (Object payload : new Object[]{null, "invalid", new long[]{2, 7},
                    new Integer[]{2, null}, Arrays.asList(2, null), List.of(2, "skip"),
                    List.of(5.9), List.of(2L), List.of(Long.MAX_VALUE)}) {
                assertThrows(IllegalArgumentException.class, () -> WorkerTaskCalculator.calculate(operation, payload));
            }
        }
        assertThrows(IllegalArgumentException.class, () -> WorkerTaskCalculator.calculate("SUM", new int[]{1}));
        assertThrows(IllegalArgumentException.class, () -> WorkerTaskCalculator.calculate(null, new int[]{1}));
    }

    @Test
    void primeChecksAndRangeEndpointsHandleIntegerLimits() {
        assertTrue(WorkerTaskCalculator.isPrime(Integer.MAX_VALUE));
        for (int value : new int[]{Integer.MIN_VALUE, -2, 0, 1, 4, 9, 121, 561, 2147395600}) {
            assertFalse(WorkerTaskCalculator.isPrime(value));
        }
        assertEquals(Long.valueOf(Integer.MAX_VALUE), WorkerTaskCalculator.calculate(
                "PRIMESUM", new PrimeRange(Integer.MAX_VALUE - 1, Integer.MAX_VALUE)));
        assertEquals(Integer.valueOf(1), WorkerTaskCalculator.calculate(
                "PRIMECOUNT", new PrimeRange(Integer.MAX_VALUE, Integer.MAX_VALUE)));
        assertEquals(Long.valueOf(37_550_402_023L), WorkerTaskCalculator.calculate("PRIMESUM", new PrimeRange(1, 1_000_000)));
    }
}

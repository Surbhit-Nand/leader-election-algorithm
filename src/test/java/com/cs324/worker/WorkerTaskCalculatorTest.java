package com.cs324.worker;

import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class WorkerTaskCalculatorTest {
    @Test
    void smallPrimesAndRangesCrossingZeroAreCalculatedCorrectly() {
        for (int value : new int[]{2, 3, 5, 7, 97}) {
            assertTrue(WorkerTaskCalculator.isPrime(value), "Expected prime: " + value);
        }
        assertEquals(Long.valueOf(5), WorkerTaskCalculator.calculate("PRIMESUM", new int[]{-5, 3}));
        assertEquals(Integer.valueOf(2), WorkerTaskCalculator.calculate("PRIMECOUNT", new int[]{-5, 3}));
        assertEquals(Long.valueOf(0), WorkerTaskCalculator.calculate("PRIMESUM", new int[]{-5, 1}));
        assertEquals(Integer.valueOf(0), WorkerTaskCalculator.calculate("PRIMECOUNT", new int[]{8, 10}));
    }

    @Test
    void separatePortionsCombineToTheWholeJob() {
        Integer[] first = {-9, 2};
        Integer[] second = {5, 5, 12, 17};
        Integer[] whole = {-9, 2, 5, 5, 12, 17};
        assertEquals(WorkerTaskCalculator.calculate("MAX", whole).intValue(),
                Math.max(WorkerTaskCalculator.calculate("MAX", first).intValue(),
                        WorkerTaskCalculator.calculate("MAX", second).intValue()));
        assertEquals(WorkerTaskCalculator.calculate("PRIMECOUNT", whole).intValue(),
                WorkerTaskCalculator.calculate("PRIMECOUNT", first).intValue()
                        + WorkerTaskCalculator.calculate("PRIMECOUNT", second).intValue());
        assertEquals(WorkerTaskCalculator.calculate("PRIMESUM", new int[]{1, 100}).longValue(),
                Math.addExact(WorkerTaskCalculator.calculate("PRIMESUM", new int[]{1, 50}).longValue(),
                        WorkerTaskCalculator.calculate("PRIMESUM", new int[]{51, 100}).longValue()));
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
    void primeOperationsTreatTwoPrimitiveValuesAsInclusiveRanges() {
        assertEquals(Long.valueOf(17), WorkerTaskCalculator.calculate("primesum", new int[]{1, 10}));
        assertEquals(Integer.valueOf(4), WorkerTaskCalculator.calculate("primecount", new int[]{2, 7}));
        assertEquals(Long.valueOf(7), WorkerTaskCalculator.calculate("PRIMESUM", new int[]{7, 7}));
        assertEquals(Long.valueOf(0), WorkerTaskCalculator.calculate("PRIMESUM", new int[]{10, 1}));
        assertEquals(Integer.valueOf(0), WorkerTaskCalculator.calculate("PRIMECOUNT", new int[]{10, 1}));
    }

    @Test
    void boxedArraysAndCollectionsStayListsAndRetainDuplicates() {
        assertEquals(Integer.valueOf(2), WorkerTaskCalculator.calculate("PRIMECOUNT", new Integer[]{2, 7}));
        assertEquals(Long.valueOf(9), WorkerTaskCalculator.calculate("PRIMESUM", List.of(2, 7)));
        assertEquals(Integer.valueOf(4), WorkerTaskCalculator.calculate("PRIMECOUNT", new int[]{2, 3, 4, 5, 5}));
        assertEquals(Long.valueOf(15), WorkerTaskCalculator.calculate("PRIMESUM", new int[]{2, 3, 5, 5}));
        assertEquals(Long.valueOf(7), WorkerTaskCalculator.calculate("PRIMESUM", new Integer[]{null, 2, 5}));
        assertEquals(Long.valueOf(7), WorkerTaskCalculator.calculate("PRIMESUM", Arrays.asList(null, "skip", 2L, 5.9)));
    }

    @Test
    void emptyAndUnsupportedPayloadsMatchWorkerBehavior() {
        for (Object payload : new Object[]{null, "invalid", new long[]{2, 7}, new int[0], List.of()}) {
            assertEquals(Integer.valueOf(0), WorkerTaskCalculator.calculate("MAX", payload));
            assertEquals(Integer.valueOf(0), WorkerTaskCalculator.calculate("PRIMECOUNT", payload));
            assertEquals(Long.valueOf(0), WorkerTaskCalculator.calculate("PRIMESUM", payload));
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
                "PRIMESUM", new int[]{Integer.MAX_VALUE - 1, Integer.MAX_VALUE}));
        assertEquals(Integer.valueOf(1), WorkerTaskCalculator.calculate(
                "PRIMECOUNT", new int[]{Integer.MAX_VALUE, Integer.MAX_VALUE}));
        assertEquals(Long.valueOf(37_550_402_023L), WorkerTaskCalculator.calculate("PRIMESUM", new int[]{1, 1_000_000}));
    }
}

package com.cs324.common;

import java.io.Serializable;
import java.util.Objects;

/** Carries a chunk and the coordinator that must receive its answer. */
public record TaskAssignment(Object payload, WorkerRemote coordinator) implements Serializable {
    public TaskAssignment {
        Objects.requireNonNull(coordinator, "coordinator");
    }
}

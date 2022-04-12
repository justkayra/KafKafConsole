package dev.kaira.kafka;

import io.quarkus.runtime.annotations.RegisterForReflection;

@RegisterForReflection
public class Price {
    public int id;
    public String name;
}

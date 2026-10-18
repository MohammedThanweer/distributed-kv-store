package com.raftkv;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class KeyValueStoreTest {

    private KeyValueStore store;

    @BeforeEach
    void setUp() {
        store = new KeyValueStore();
    }

    @Test
    void shouldStoreAndRetrieveValue() {
        store.put("user101", "Alice");

        assertEquals("Alice", store.get("user101").orElse(null));
    }

    @Test
    void shouldReturnEmptyForMissingKey() {
        assertTrue(store.get("unknown").isEmpty());
    }

    @Test
    void shouldUpdateExistingValue() {
        store.put("user101", "Alice");
        store.put("user101", "Bob");

        assertEquals("Bob", store.get("user101").orElse(null));
    }

    @Test
    void shouldDeleteExistingKey() {
        store.put("user101", "Alice");

        assertTrue(store.delete("user101"));
        assertTrue(store.get("user101").isEmpty());
    }

    @Test
    void shouldReturnFalseWhenDeletingMissingKey() {
        assertFalse(store.delete("unknown"));
    }
}

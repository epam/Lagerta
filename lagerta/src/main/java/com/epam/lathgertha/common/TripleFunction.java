package com.epam.lathgertha.common;

import java.util.Objects;
import java.util.function.Function;

/**
 * @author Andrei_Yakushin
 * @since 01.04.2017 8:02
 */
@FunctionalInterface
public interface TripleFunction<A, B, C, R> {
    R apply(A a, B b, C c);

    default <V> TripleFunction<A, B, C, V> andThen(Function<? super R, ? extends V> after) {
        Objects.requireNonNull(after);
        return (a, b, c) -> after.apply(apply(a, b, c));
    }
}

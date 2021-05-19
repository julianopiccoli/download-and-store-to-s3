package br.jpiccoli;

@FunctionalInterface
public interface RetryHandler {

    boolean shouldRetry(Exception e);

}

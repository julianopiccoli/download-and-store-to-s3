package br.jpiccoli;

class DefaultRetryHandler implements  RetryHandler {

    @Override
    public boolean shouldRetry(Exception e) {
        return true;
    }

}

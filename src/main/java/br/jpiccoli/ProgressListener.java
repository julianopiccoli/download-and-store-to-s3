package br.jpiccoli;

public interface ProgressListener {

    void transferProgress(long totalTransferred, long contentLength);

    void transferRateUpdate(int bytesTransferred, long timeDeltaInMs);

}

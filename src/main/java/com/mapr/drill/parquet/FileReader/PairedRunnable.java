package com.mapr.drill.parquet.FileReader;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Created by pchandra on 7/12/16.
 */
public class PairedRunnable implements Callable{

  Callable first;
  Callable second;
  ExecutorService firstPool;
  ExecutorService secondPool;

  public PairedRunnable(Callable first, Callable second, ExecutorService firstPool,
      ExecutorService secondPool) {
    this.first = first;
    this.second = second;
    this.firstPool = firstPool;
    this.secondPool = secondPool;
  }


  @Override public PairedFuture call() throws Exception {
    Future firstFuture = firstPool.submit(first);
    Future secondFuture = secondPool.submit(second);
    return  new PairedFuture(firstFuture, secondFuture);
  }

  public static class PairedFuture {
    public Future  first;
    public Future  second;
    PairedFuture(Future first, Future second){
      this.first = first;
      this.second = second;
    }
  }

}

package com.zws;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;

/**
 * @author zhengws
 * @date 2021-10-17
 */
public class CompletableFutureTest {
    public static void main(String[] args) {
        CompletableFuture<Object> future = new CompletableFuture<>();
        future.thenAccept(new Consumer<Object>() {
            @Override
            public void accept(Object o) {
                System.out.println(Thread.currentThread().getName() + ", thenAccept: " + System.currentTimeMillis());
            }
        });

        future.thenAcceptAsync(new Consumer<Object>() {
            @Override
            public void accept(Object o) {
                System.out.println(Thread.currentThread().getName() + ", thenAcceptAsync: " + System.currentTimeMillis());
            }
        }, Executors.newFixedThreadPool(1, new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setDaemon(true);
                return thread;
            }
        }));

        System.out.println("main .....");
        future.complete("hahahaha");
    }
}

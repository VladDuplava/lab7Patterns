import io.reactivex.rxjava3.core.Observable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Task72RetryBackoff {
    public static void main(String[] args) {
        AtomicInteger attempts = new AtomicInteger();
        Observable<String> unstableApiCall = Observable.defer(() -> Observable.create(emitter -> {
            int attempt = attempts.incrementAndGet();
            System.out.println("[ПОВТОР] Спроба #" + attempt);
            if (attempt < 4) {
                emitter.onError(new IOException("Connection timeout"));
            } else {
                emitter.onNext("(+) Відповідь API: {status: 'ok', data: [...]}");
                emitter.onComplete();
            }
        }));

        unstableApiCall.retryWhen(errors ->
                        errors.zipWith(Observable.range(1, 4), (e, n) -> n)
                                .flatMap(n -> {
                                    if (n >= 4) return Observable.error(new RuntimeException("Після 4 спроб запит не вдався"));
                                    long delay = 1L << (n - 1);
                                    System.out.println("Очікуємо " + delay + " сек перед повтором...");
                                    return Observable.timer(delay, TimeUnit.SECONDS);
                                })
                )
                .blockingSubscribe(
                        System.out::println,
                        e -> System.out.println("(-) Фінальна помилка: " + e.getMessage())
                );
    }
}

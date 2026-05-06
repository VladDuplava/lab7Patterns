import io.reactivex.rxjava3.core.BackpressureStrategy;
import io.reactivex.rxjava3.core.Flowable;
import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class Task62FlowableBackpressure {
    public static void main(String[] args) {
        List<String> eventList = Arrays.asList(
                "LOGIN:user1", "CLICK:btn_buy", "VIEW:product_42",
                "LOGIN:user2", "LOGOUT:user1", "CLICK:btn_cart",
                "VIEW:product_7", "LOGIN:user3", "CLICK:btn_pay",
                "LOGOUT:user2", "LOGIN:user4", "VIEW:product_1"
        );
        Observable<String> events = Observable.fromIterable(eventList);

        AtomicInteger batch = new AtomicInteger();
        events.buffer(5).blockingSubscribe(g ->
                System.out.println("[DB] Batch INSERT #" + batch.incrementAndGet() + ": " + g));
        System.out.println("(+) Збережено подій: 12");

        int total = 1000;
        AtomicInteger processed = new AtomicInteger();
        Flowable.<Integer>create(emitter -> {
            for (int i = 1; i <= total; i++) emitter.onNext(i);
            emitter.onComplete();
        }, BackpressureStrategy.DROP)
                .observeOn(Schedulers.computation(), false, 128)
                .doOnNext(i -> {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    processed.incrementAndGet();
                })
                .blockingSubscribe();

        int dropped = total - processed.get();
        System.out.println("[ЗВІТ] Оброблено: ~" + processed.get());
        System.out.println("[ЗВІТ] Відкинуто: ~" + dropped);
        System.out.println("(!) Стратегія DROP: частину елементів втрачено");
    }
}

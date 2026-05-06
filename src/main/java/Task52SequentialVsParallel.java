import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.Arrays;
import java.util.List;

public class Task52SequentialVsParallel {
    record ServiceCall(String serviceName, int delayMs) {}

    static String thread() { return Thread.currentThread().getName(); }

    static Observable<String> call(ServiceCall s) {
        return Observable.fromCallable(() -> {
            Thread.sleep(s.delayMs());
            return String.format("[%s] (+) %s відповів за %d мс", thread(), s.serviceName(), s.delayMs());
        }).subscribeOn(Schedulers.io());
    }

    public static void main(String[] args) {
        List<ServiceCall> services = Arrays.asList(
                new ServiceCall("UserService", 800),
                new ServiceCall("OrderService", 1200),
                new ServiceCall("RecommendationService", 600)
        );

        long s1 = System.currentTimeMillis();
        Observable.fromIterable(services).concatMap(Task52SequentialVsParallel::call).blockingSubscribe(System.out::println);
        System.out.println("Загальний час (послідовно): ~" + (System.currentTimeMillis() - s1) + " мс");

        long s2 = System.currentTimeMillis();
        Observable.fromIterable(services).flatMap(Task52SequentialVsParallel::call).blockingSubscribe(System.out::println);
        System.out.println("Загальний час (паралельно): ~" + (System.currentTimeMillis() - s2) + " мс");
    }
}

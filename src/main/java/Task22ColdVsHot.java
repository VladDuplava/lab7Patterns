import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.observables.ConnectableObservable;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Task22ColdVsHot {
    public static void main(String[] args) throws Exception {
        List<String> results = Arrays.asList(
                "Динамо 2:1 Шахтар",
                "Шахтар 3:0 Металіст",
                "Зоря 1:1 Дніпро",
                "Карпати 0:2 Динамо",
                "Металіст 2:2 Зоря"
        );

        Observable<String> cold = Observable.fromIterable(results);
        cold.subscribe(v -> System.out.println("[Cold #1] " + v));
        cold.subscribe(v -> System.out.println("[Cold #2] " + v));

        ConnectableObservable<String> hot = Observable.interval(1, TimeUnit.SECONDS)
                .take(results.size())
                .map(i -> results.get(i.intValue()))
                .publish();

        hot.subscribe(v -> System.out.println("[Hot #1] " + v));
        hot.connect();
        Thread.sleep(2000);
        hot.subscribe(v -> System.out.println("[Hot #2 delayed] " + v));
        Thread.sleep(5000);
    }
}

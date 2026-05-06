import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class Task32FlatMapConcatMap {
    record FoodOrder(String orderId, List<String> items) {}

    public static void main(String[] args) {
        List<FoodOrder> orders = Arrays.asList(
                new FoodOrder("ZAM-01", Arrays.asList("Піца Маргарита", "Кола 0.5л")),
                new FoodOrder("ZAM-02", Arrays.asList("Борщ", "Вареники", "Компот")),
                new FoodOrder("ZAM-03", Arrays.asList("Суші-сет 20шт", "Місо-суп"))
        );

        Observable.fromIterable(orders)
                .flatMap(o -> Observable.fromIterable(o.items()))
                .blockingSubscribe(i -> System.out.println(">> " + i));

        System.out.println("-- flatMap delay --");
        Observable.fromIterable(orders)
                .flatMap(o -> Observable.timer(500, TimeUnit.MILLISECONDS).map(x -> o.orderId()).subscribeOn(Schedulers.computation()))
                .blockingSubscribe(System.out::println);

        System.out.println("-- concatMap delay --");
        Observable.fromIterable(orders)
                .concatMap(o -> Observable.timer(500, TimeUnit.MILLISECONDS).map(x -> o.orderId()))
                .blockingSubscribe(System.out::println);
    }
}

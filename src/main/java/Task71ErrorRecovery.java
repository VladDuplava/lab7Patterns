import io.reactivex.rxjava3.core.Observable;

public class Task71ErrorRecovery {
    public static void main(String[] args) {
        Observable<String> currencyService = Observable.create(emitter -> {
            emitter.onNext("USD -> UAH: 41.50");
            emitter.onNext("EUR -> UAH: 44.20");
            emitter.onError(new RuntimeException("Сервіс тимчасово недоступний"));
        });

        System.out.println("-- Scenario A: onErrorReturn --");
        currencyService
                .onErrorReturn(e -> "Використовується кешований курс: USD -> UAH: 41.00")
                .blockingSubscribe(System.out::println);

        System.out.println("-- Scenario B: onErrorResumeNext --");
        currencyService
                .onErrorResumeNext(e -> Observable.just("JPY -> UAH: 0.27", "PLN -> UAH: 10.30"))
                .blockingSubscribe(System.out::println);
    }
}

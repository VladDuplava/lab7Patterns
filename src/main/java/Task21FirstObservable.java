import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.core.Observer;
import io.reactivex.rxjava3.disposables.Disposable;

public class Task21FirstObservable {
    public static void main(String[] args) {
        Observable<String> atm = Observable.just(
                "Вставте картку",
                "Введіть PIN-код",
                "Оберіть суму: 500 грн",
                "Видача готівки...",
                "Дякуємо! Заберіть картку"
        );

        atm.subscribe(new Observer<>() {
            @Override public void onSubscribe(Disposable d) { System.out.println("[БАНКОМАТ] Сесію розпочато"); }
            @Override public void onNext(String s) { System.out.println(">> " + s); }
            @Override public void onError(Throwable e) { System.out.println("[БАНКОМАТ] Помилка: " + e.getMessage()); }
            @Override public void onComplete() { System.out.println("[БАНКОМАТ] Сесію завершено"); }
        });
    }
}

import io.reactivex.rxjava3.core.Completable;
import io.reactivex.rxjava3.core.Maybe;
import io.reactivex.rxjava3.core.Single;

public class Task42MaybeCompletable {
    static Maybe<String> findInCache(String key) {
        return switch (key) {
            case "user:1" -> Maybe.just("{'name':'Леся','age':28}");
            case "user:2" -> Maybe.empty();
            case "user:error" -> Maybe.error(new RuntimeException("Redis недоступний"));
            default -> Maybe.empty();
        };
    }

    static Completable validateInput() {
        return Completable.fromAction(() -> {
            System.out.println("[ПОШУК] Перевірка даних...");
            System.out.println("(+) Дані валідні");
        });
    }

    static Completable saveToDatabase(boolean fail) {
        return Completable.fromAction(() -> {
            System.out.println("[DB] Збереження в БД...");
            if (fail) throw new RuntimeException("Помилка збереження в БД");
            System.out.println("(+) Збережено");
        });
    }

    static Single<String> generateToken() {
        return Single.just("eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.demo");
    }

    public static void main(String[] args) {
        findInCache("user:1").subscribe(
                v -> System.out.println("[КЕШ (+)] Знайдено: " + v),
                e -> System.out.println("[КЕШ (!)] Помилка: " + e.getMessage())
        );
        findInCache("user:2").defaultIfEmpty("Завантажено з БД")
                .subscribe(v -> System.out.println("[КЕШ (-)] Кеш-міс. Значення: " + v));
        findInCache("user:error").subscribe(
                v -> System.out.println(v),
                e -> System.out.println("[КЕШ (!)] Помилка: " + e.getMessage())
        );

        validateInput().andThen(saveToDatabase(false)).andThen(generateToken()).subscribe(
                t -> { System.out.println("[ТОКЕН] Токен: " + t); System.out.println("(+) Реєстрацію завершено успішно!"); },
                e -> System.out.println("(-) Реєстрацію зірвано: " + e.getMessage())
        );
        validateInput().andThen(saveToDatabase(true)).andThen(generateToken()).subscribe(
                t -> System.out.println(t),
                e -> System.out.println("(-) Реєстрацію зірвано: " + e.getMessage())
        );
    }
}

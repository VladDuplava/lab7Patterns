import io.reactivex.rxjava3.core.Single;

public class Task41Single {
    static Single<String> getUserById(int id) {
        if (id > 0) return Single.just("Користувач #" + id + ": Іван Франко");
        return Single.error(new IllegalArgumentException("ID не може бути від'ємним або нульовим"));
    }

    public static void main(String[] args) {
        getUserById(42).subscribe(
                v -> System.out.println("(+) Знайдено: " + v),
                e -> System.out.println("(-) Помилка: " + e.getMessage())
        );
        getUserById(-1).subscribe(
                v -> System.out.println("(+) Знайдено: " + v),
                e -> System.out.println("(-) Помилка: " + e.getMessage())
        );
    }
}

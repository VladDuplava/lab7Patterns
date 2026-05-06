import io.reactivex.rxjava3.core.Observable;
import java.text.Collator;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class Task12CompareApproaches {
    public static void main(String[] args) {
        List<String> cities = Arrays.asList(
                "Київ", "Харків", "Одеса", "Дніпро", "Запоріжжя",
                "Кривий Ріг", "Миколаїв", "Херсон", "Кропивницький",
                "Черкаси", "Суми", "Хмельницький", "Чернівці", "Каховка"
        );
        Locale ua = Locale.forLanguageTag("uk-UA");
        Collator collator = Collator.getInstance(ua);

        List<String> imperative = new ArrayList<>();
        for (String city : cities) {
            if (city.startsWith("К")) imperative.add(city.toUpperCase(ua));
        }
        imperative.sort(collator);
        imperative.forEach(System.out::println);

        System.out.println("-- stream --");
        cities.stream().filter(c -> c.startsWith("К")).map(c -> c.toUpperCase(ua)).sorted(collator).forEach(System.out::println);

        System.out.println("-- rxjava --");
        Observable.fromIterable(cities)
                .filter(c -> c.startsWith("К"))
                .map(c -> c.toUpperCase(ua))
                .toSortedList(collator)
                .flattenAsObservable(x -> x)
                .blockingSubscribe(System.out::println);
    }
}

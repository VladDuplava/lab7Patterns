import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.schedulers.Schedulers;

public class Task51Schedulers {
    static String thread() { return Thread.currentThread().getName(); }

    static String downloadImage(String image) throws InterruptedException {
        Thread.sleep(1000);
        System.out.printf("[%s] [ЗАВАНТ] Завантаження: %s%n", thread(), image);
        return image;
    }

    static String compressImage(String image) throws InterruptedException {
        Thread.sleep(500);
        System.out.printf("[%s] [СТИСК] Стиснення: %s%n", thread(), image);
        return image;
    }

    static void displayImage(String image) {
        System.out.printf("[%s] [ФОТО] Відображення: %s%n", thread(), image);
    }

    public static void main(String[] args) {
        Observable.just("photo_1.jpg", "photo_2.jpg", "photo_3.jpg")
                .observeOn(Schedulers.io())
                .map(i -> {
                    try { return downloadImage(i); } catch (InterruptedException e) { throw new RuntimeException(e); }
                })
                .observeOn(Schedulers.computation())
                .map(i -> {
                    try { return compressImage(i); } catch (InterruptedException e) { throw new RuntimeException(e); }
                })
                .observeOn(Schedulers.trampoline())
                .blockingSubscribe(Task51Schedulers::displayImage);
    }
}

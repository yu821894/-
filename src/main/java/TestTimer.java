import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class TestTimer {

    private static int n = 0;

    public static void main(String[] args){


        Runnable runnable = new Runnable() {
            @Override
            public void run() {
              n++;
              System.out.println(n);
            }
        };
        ScheduledExecutorService service = Executors
                .newSingleThreadScheduledExecutor();
        // 第二个参数为首次执行的延时时间，第三个参数为定时执行的间隔时间
        service.scheduleAtFixedRate(runnable, 0, 1, TimeUnit.SECONDS);

    }
}

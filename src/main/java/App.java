import com.domain.Weight;
import com.service.ShowPackageBoxSum;
import com.serviceImpl.PackageBoxSum;
import com.unit.IndexUnit;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import io.vertx.ext.web.handler.BodyHandler;

import java.io.File;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class App extends AbstractVerticle {


    private final static String INDEX_DIR = "D:\\idea\\indexplace";

    public static void main(String[] args){

        File f = new File(INDEX_DIR);
        if (f.listFiles().length == 0){
            //制作全部索引
            IndexUnit.createAllIndex();

            //启动定时更新线程
            App.startUpdate();



        }else {
            //启动定时更新线程
            App.startUpdate();
        }

        //启动服务
        //设置主线程阻塞时间
        VertxOptions vertxOptions = new VertxOptions();
        vertxOptions.setBlockedThreadCheckInterval(9999999);
        vertxOptions.setWorkerPoolSize(100);
        Vertx vertx = Vertx.vertx(vertxOptions);

        vertx.deployVerticle(new App());

    }

    @Override
    public void start() {

        //实例化一个路由，用来路由不同的接口
        Router router = Router.router(vertx);

        //增加一个处理器，将请求的上下文信息放入RoutingContext对象中
        router.route().handler(BodyHandler.create());



        //处理post方法的路由
        router.post("/getData").handler(this::handlePost);

        //创建httpserver，分发路由，监听8080端口
        vertx.createHttpServer().requestHandler(router::accept).listen(8080);
    }

    private void handlePost(RoutingContext routingContext){
        JsonObject j1 =routingContext.getBodyAsJson();
        String startTime = j1.getString("startTime");
        String endTime = j1.getString("endTime");

        if (isBlank(startTime) || isBlank(endTime)){
            routingContext.response().setStatusCode(400).end();
        }

        ShowPackageBoxSum s = new PackageBoxSum();
        Weight w = s.show_PackageBoxSum(startTime,endTime);
        JsonObject jsonObject = new JsonObject();
        jsonObject.put("净重",w.getWeight()).put("双a率",Double.parseDouble(w.getDoubleARate())*100+"%")
                .put("已打包数",w.getPackageCount());
        routingContext.response().putHeader("content-type","application/json")
                .end(jsonObject.encodePrettily());

    }

    private boolean isBlank(String str){

        if (str == null || "".equals(str)){
            return true;
        }
        return false;
    }
    //启动定时更新
    private static void startUpdate(){
        Runnable runnable = new Runnable() {
            @Override
            public void run() {
                //更新索引
                IndexUnit.updateIndex();

            }
        };
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(runnable,600,1800, TimeUnit.SECONDS);
    }
}

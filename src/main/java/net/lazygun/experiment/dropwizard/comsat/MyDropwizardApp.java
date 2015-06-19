package net.lazygun.experiment.dropwizard.comsat;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.fibers.dropwizard.FiberApplication;
import co.paralleluniverse.fibers.dropwizard.FiberDBIFactory;
import co.paralleluniverse.fibers.dropwizard.FiberHttpClientBuilder;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.io.Resources;
import io.dropwizard.Configuration;
import io.dropwizard.client.HttpClientConfiguration;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.setup.Environment;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.hibernate.validator.constraints.Length;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.sqlobject.Bind;
import org.skife.jdbi.v2.sqlobject.SqlQuery;
import org.skife.jdbi.v2.sqlobject.SqlUpdate;
import org.skife.jdbi.v2.util.StringMapper;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class MyDropwizardApp extends FiberApplication<MyDropwizardApp.MyConfig> {
    private IDBI jdbi;
    private MyDAO dao;
    private HttpClient httpClient;

    public static void main(String[] args) throws Exception {
        new MyDropwizardApp().run(new String[]{"server", Resources.getResource("server.yml").getPath()});
    }

    @Override
    public void fiberRun(MyConfig config, Environment env) throws Exception {
        httpClient = new FiberHttpClientBuilder(env)
                .using(config.getHttpClientConfiguration()).build("MyClient");
        jdbi = new FiberDBIFactory().build(env, config.getDataSourceFactory(), "MyDB");
        dao = jdbi.onDemand(MyDAO.class);
        env.jersey().register(MY_RESOURCE_OBJ);
    }

    public static class MyConfig extends Configuration {
        @Valid
        @NotNull
        @JsonProperty
        private final HttpClientConfiguration httpClient = new HttpClientConfiguration();

        @Valid
        @NotNull
        @JsonProperty
        private final DataSourceFactory database = new DataSourceFactory();

        public HttpClientConfiguration getHttpClientConfiguration() {
            return httpClient;
        }

        public DataSourceFactory getDataSourceFactory() {
            return database;
        }
    }

    public static class Saying {
        private long id;

        @Length(max = 3)
        private String content;

        public Saying() {
        }

        public Saying(long id, String content) {
            this.id = id;
            this.content = content;
        }

        @JsonProperty
        public long getId() {
            return id;
        }

        @JsonProperty
        public String getContent() {
            return content;
        }
    }

    @Suspendable
    public interface MyDAO {
        @SqlUpdate("create table if not exists something (id int primary key, name varchar(100)")
        void createSomethingTable();

        @SqlUpdate("drop something table")
        void dropSomethingTable();

        @SqlUpdate("insert into something (id, name) values (:id, :name)")
        void insert(@Bind("id") int id, @Bind("name") String name);

        @SqlQuery("select name from something where id = :id")
        String findNameById(@Bind("id") int id);
    }

    private final AtomicInteger ai = new AtomicInteger();

    @GET
    @Timed
    public Saying get(@QueryParam("name") Optional<String> name,
                      @QueryParam("sleep") Optional<Integer> sleepParameter) throws InterruptedException, SuspendExecution {
        Fiber.sleep(sleepParameter.orElse(10));
        return new Saying(ai.incrementAndGet(), name.orElse("name"));
    }

    @GET
    @Path("/http")
    @Timed
    public String http(@QueryParam("name") Optional<String> name) throws InterruptedException, SuspendExecution, IOException {
        return httpClient.execute(new HttpGet("http://localhost:8080/?sleep=10&name=" + name.orElse("name")),
                                  new BasicResponseHandler());
    }

    @GET
    @Path("/fluent")
    @Timed
    public Integer fluentAPI(
            @QueryParam("id") Optional<Integer> id) throws InterruptedException, SuspendExecution, IOException {
        try (Handle h = jdbi.open()) {
            h.execute("create table if not exists fluentAPI (id int primary key, name varchar(100))");
            for (int i = 0; i < 100; i++)
                h.execute("insert into fluidAPI (id, name) values (?, ?)", i, "stranger " + i);
            int size = h.createQuery("select name from fluentAPI where id < :id order by id")
                    .bind("id", id.orElse(50)).map(StringMapper.FIRST).list().size();
            h.execute("drop table fluentAPI");
            return size;
        }
    }

    @GET
    @Path("/dao")
    @Timed
    public String dao(
            @QueryParam("id") Optional<Integer> id) throws InterruptedException, SuspendExecution, IOException {
        dao.createSomethingTable();
        for (int i = 0; i < 100; i++) dao.insert(i, "name" + i);
        final String result = dao.findNameById(37);
        dao.dropSomethingTable();
        return result;
    }

    private Object MY_RESOURCE_OBJ = this;
}

package net.lazygun.experiment.dropwizard.comsat;

import co.paralleluniverse.fibers.Fiber;
import co.paralleluniverse.fibers.SuspendExecution;
import co.paralleluniverse.fibers.Suspendable;
import co.paralleluniverse.fibers.dropwizard.FiberApplication;
import co.paralleluniverse.fibers.dropwizard.FiberDBIFactory;
import co.paralleluniverse.fibers.dropwizard.FiberHttpClientBuilder;
import com.codahale.metrics.annotation.Timed;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Optional;
import com.google.common.io.Resources;
import io.dropwizard.Configuration;
import io.dropwizard.client.HttpClientConfiguration;
import io.dropwizard.db.DataSourceFactory;
import io.dropwizard.java8.Java8Bundle;
import io.dropwizard.setup.Bootstrap;
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

import javax.inject.Singleton;
import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Singleton
@Path("/")
@Produces(MediaType.APPLICATION_JSON)
public class MyDropwizardApp extends FiberApplication<MyDropwizardApp.MyConfig> {
    private IDBI jdbi;
    private MyDAO dao;
    private HttpClient httpClient;

    public static void main(String[] args) throws Exception {
        new MyDropwizardApp().run("server", Resources.getResource("server.yml").getPath());
    }

    @Override
    public void initialize(Bootstrap<MyConfig> bootstrap) {
        bootstrap.addBundle(new Java8Bundle());
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
        @SqlUpdate("create table if not exists something (id int primary key, name varchar(100))")
        void createSomethingTable();

        @SqlUpdate("drop table something")
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
        Fiber.sleep(sleepParameter.or(10));
        return new Saying(ai.incrementAndGet(), name.or("name"));
    }

    @GET
    @Path("/http")
    @Timed
    public String http(@QueryParam("name") Optional<String> name) throws InterruptedException, SuspendExecution, IOException {
        return httpClient.execute(new HttpGet("http://localhost:8080/?sleep=10&name=" + name.or("name")),
                                  new BasicResponseHandler());
    }

    @GET
    @Path("/fluent")
    @Timed
    public List<String> fluentAPI(
            @QueryParam("id") Optional<Integer> id) throws InterruptedException, SuspendExecution, IOException {
        try (Handle h = jdbi.open()) {
            try {
                h.execute("create table if not exists fluentAPI (id int primary key, name varchar(100))");
                for (int i = 0; i < 100; i++)
                    h.execute("insert into fluentAPI (id, name) values (?, ?)", i, "stranger " + i);
                return h.createQuery("select name from fluentAPI where id < :id order by id")
                        .bind("id", id.or(50)).map(StringMapper.FIRST).list();
            } finally {
                h.execute("drop table fluentAPI");
            }
        }
    }

    @GET
    @Path("/dao")
    @Timed
    public List<String> dao(
            @QueryParam("id") Optional<Integer> id) throws InterruptedException, SuspendExecution, IOException {
        final String result;
        try {
            dao.createSomethingTable();
            for (int i = 0; i < 100; i++) dao.insert(i, "name" + i);
            result = dao.findNameById(id.or(0));
        } finally {
            dao.dropSomethingTable();
        }
        return Collections.singletonList(result);
    }

    private Object MY_RESOURCE_OBJ = this;
}

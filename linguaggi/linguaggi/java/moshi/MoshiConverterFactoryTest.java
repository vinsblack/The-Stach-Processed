/*
 * Copyright (C) 2015 Square, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package retrofit2.converter.moshi;

import static com.google.common.truth.Truth.assertThat;
import static java.lang.annotation.RetentionPolicy.RUNTIME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import com.google.testing.junit.testparameterinjector.TestParameter;
import com.google.testing.junit.testparameterinjector.TestParameterInjector;
import com.squareup.moshi.FromJson;
import com.squareup.moshi.JsonDataException;
import com.squareup.moshi.JsonQualifier;
import com.squareup.moshi.JsonReader;
import com.squareup.moshi.JsonWriter;
import com.squareup.moshi.Moshi;
import com.squareup.moshi.ToJson;
import java.io.EOFException;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;
import okio.ByteString;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import retrofit2.Call;
import retrofit2.Callback;
import retrofit2.Response;
import retrofit2.Retrofit;
import retrofit2.http.Body;
import retrofit2.http.GET;
import retrofit2.http.POST;

@RunWith(TestParameterInjector.class)
public final class MoshiConverterFactoryTest {
  @Retention(RUNTIME)
  @JsonQualifier
  @interface Qualifier {}

  @Retention(RUNTIME)
  @interface NonQualifer {}

  interface AnInterface {
    String getName();
  }

  static class AnImplementation implements AnInterface {
    private final String theName;

    AnImplementation(String name) {
      theName = name;
    }

    @Override
    public String getName() {
      return theName;
    }
  }

  static final class ErroringValue {
    final String theName;

    ErroringValue(String theName) {
      this.theName = theName;
    }
  }

  static class Adapters {
    @ToJson
    public void write(JsonWriter jsonWriter, AnInterface anInterface) throws IOException {
      jsonWriter.beginObject();
      jsonWriter.name("name").value(anInterface.getName());
      jsonWriter.endObject();
    }

    @FromJson
    public AnInterface read(JsonReader jsonReader) throws IOException {
      jsonReader.beginObject();

      String name = null;
      while (jsonReader.hasNext()) {
        switch (jsonReader.nextName()) {
          case "name":
            name = jsonReader.nextString();
            break;
        }
      }

      jsonReader.endObject();
      return new AnImplementation(name);
    }

    @ToJson
    public void write(JsonWriter writer, @Qualifier String value) throws IOException {
      writer.value("qualified!");
    }

    @FromJson
    @Qualifier
    public String readQualified(JsonReader reader) throws IOException {
      String string = reader.nextString();
      if (string.equals("qualified!")) {
        return "it worked!";
      }
      throw new AssertionError("Found: " + string);
    }

    @FromJson
    public ErroringValue readWithoutEndingObject(JsonReader reader) throws IOException {
      reader.beginObject();
      reader.skipName();
      String theName = reader.nextString();
      return new ErroringValue(theName);
    }

    @ToJson
    public void write(JsonWriter writer, ErroringValue value) throws IOException {
      throw new EOFException("oops!");
    }
  }

  interface Service {
    @POST("/")
    Call<AnImplementation> anImplementation(@Body AnImplementation impl);

    @POST("/")
    Call<AnInterface> anInterface(@Body AnInterface impl);

    @GET("/")
    Call<ErroringValue> readErroringValue();

    @POST("/")
    Call<Void> writeErroringValue(@Body ErroringValue value);

    @POST("/")
    @Qualifier
    @NonQualifer //
    Call<String> annotations(@Body @Qualifier @NonQualifer String body);
  }

  @Rule public final MockWebServer server = new MockWebServer();

  private final Service service;
  private final Service serviceLenient;
  private final Service serviceNulls;
  private final Service serviceFailOnUnknown;
  private final boolean streaming;

  public MoshiConverterFactoryTest(@TestParameter boolean streaming) {
    this.streaming = streaming;

    Moshi moshi =
        new Moshi.Builder()
            .add(
                (type, annotations, moshi1) -> {
                  for (Annotation annotation : annotations) {
                    if (!annotation.annotationType().isAnnotationPresent(JsonQualifier.class)) {
                      throw new AssertionError("Non-@JsonQualifier annotation: " + annotation);
                    }
                  }
                  return null;
                })
            .add(new Adapters())
            .build();

    MoshiConverterFactory factory = MoshiConverterFactory.create(moshi);
    if (streaming) {
      factory = factory.withStreaming();
    }

    MoshiConverterFactory factoryLenient = factory.asLenient();
    MoshiConverterFactory factoryNulls = factory.withNullSerialization();
    MoshiConverterFactory factoryFailOnUnknown = factory.failOnUnknown();
    Retrofit retrofit =
        new Retrofit.Builder().baseUrl(server.url("/")).addConverterFactory(factory).build();
    Retrofit retrofitLenient =
        new Retrofit.Builder().baseUrl(server.url("/")).addConverterFactory(factoryLenient).build();
    Retrofit retrofitNulls =
        new Retrofit.Builder().baseUrl(server.url("/")).addConverterFactory(factoryNulls).build();
    Retrofit retrofitFailOnUnknown =
        new Retrofit.Builder()
            .baseUrl(server.url("/"))
            .addConverterFactory(factoryFailOnUnknown)
            .build();
    service = retrofit.create(Service.class);
    serviceLenient = retrofitLenient.create(Service.class);
    serviceNulls = retrofitNulls.create(Service.class);
    serviceFailOnUnknown = retrofitFailOnUnknown.create(Service.class);
  }

  @Test
  public void anInterface() throws IOException, InterruptedException {
    server.enqueue(new MockResponse().setBody("{\"name\":\"value\"}"));

    Call<AnInterface> call = service.anInterface(new AnImplementation("value"));
    Response<AnInterface> response = call.execute();
    AnInterface body = response.body();
    assertThat(body.getName()).isEqualTo("value");

    RecordedRequest request = server.takeRequest();
    assertThat(request.getBody().readUtf8()).isEqualTo("{\"name\":\"value\"}");
    assertThat(request.getHeader("Content-Type")).isEqualTo("application/json; charset=UTF-8");
  }

  @Test
  public void anImplementation() throws IOException, InterruptedException {
    server.enqueue(new MockResponse().setBody("{\"theName\":\"value\"}"));

    Call<AnImplementation> call = service.anImplementation(new AnImplementation("value"));
    Response<AnImplementation> response = call.execute();
    AnImplementation body = response.body();
    assertThat(body.theName).isEqualTo("value");

    RecordedRequest request = server.takeRequest();
    assertThat(request.getBody().readUtf8()).isEqualTo("{\"theName\":\"value\"}");
    assertThat(request.getHeader("Content-Type")).isEqualTo("application/json; charset=UTF-8");
  }

  @Test
  public void annotations() throws IOException, InterruptedException {
    server.enqueue(new MockResponse().setBody("\"qualified!\""));

    Call<String> call = service.annotations("value");
    Response<String> response = call.execute();
    assertThat(response.body()).isEqualTo("it worked!");

    RecordedRequest request = server.takeRequest();
    assertThat(request.getBody().readUtf8()).isEqualTo("\"qualified!\"");
    assertThat(request.getHeader("Content-Type")).isEqualTo("application/json; charset=UTF-8");
  }

  @Test
  public void asLenient() throws IOException, InterruptedException {
    MockResponse malformedResponse = new MockResponse().setBody("{\"theName\":value}");
    server.enqueue(malformedResponse);
    server.enqueue(malformedResponse);

    Call<AnImplementation> call = service.anImplementation(new AnImplementation("value"));
    try {
      call.execute();
      fail();
    } catch (IOException e) {
      assertEquals(
          e.getMessage(),
          "Use JsonReader.setLenient(true) to accept malformed JSON at path $.theName");
    }

    Call<AnImplementation> call2 = serviceLenient.anImplementation(new AnImplementation("value"));
    Response<AnImplementation> response = call2.execute();
    AnImplementation body = response.body();
    assertThat(body.theName).isEqualTo("value");
  }

  @Test
  public void withNulls() throws IOException, InterruptedException {
    server.enqueue(new MockResponse().setBody("{}"));

    Call<AnImplementation> call = serviceNulls.anImplementation(new AnImplementation(null));
    call.execute();
    assertEquals("{\"theName\":null}", server.takeRequest().getBody().readUtf8());
  }

  @Test
  public void failOnUnknown() throws IOException, InterruptedException {
    server.enqueue(new MockResponse().setBody("{\"taco\":\"delicious\"}"));

    Call<AnImplementation> call = serviceFailOnUnknown.anImplementation(new AnImplementation(null));
    try {
      call.execute();
      fail();
    } catch (JsonDataException e) {
      assertThat(e).hasMessageThat().isEqualTo("Cannot skip unexpected NAME at $.taco");
    }
  }

  @Test
  public void utf8BomSkipped() throws IOException {
    Buffer responseBody =
        new Buffer().write(ByteString.decodeHex("EFBBBF")).writeUtf8("{\"theName\":\"value\"}");
    MockResponse malformedResponse = new MockResponse().setBody(responseBody);
    server.enqueue(malformedResponse);

    Call<AnImplementation> call = service.anImplementation(new AnImplementation("value"));
    Response<AnImplementation> response = call.execute();
    AnImplementation body = response.body();
    assertThat(body.theName).isEqualTo("value");
  }

  @Test
  public void nonUtf8BomIsNotSkipped() throws IOException {
    Buffer responseBody =
        new Buffer()
            .write(ByteString.decodeHex("FEFF"))
            .writeString("{\"theName\":\"value\"}", StandardCharsets.UTF_16);
    MockResponse malformedResponse = new MockResponse().setBody(responseBody);
    server.enqueue(malformedResponse);

    Call<AnImplementation> call = service.anImplementation(new AnImplementation("value"));
    try {
      call.execute();
      fail();
    } catch (IOException expected) {
    }
  }

  @Test
  public void requireFullResponseDocumentConsumption() throws Exception {
    server.enqueue(new MockResponse().setBody("{\"theName\":\"value\"}"));

    Call<ErroringValue> call = service.readErroringValue();
    try {
      call.execute();
      fail();
    } catch (JsonDataException e) {
      assertThat(e).hasMessageThat().isEqualTo("JSON document was not fully consumed.");
    }
  }

  @Test
  public void serializeIsStreamed() throws InterruptedException {
    assumeTrue(streaming);

    Call<Void> call = service.writeErroringValue(new ErroringValue("hi"));

    final AtomicReference<Throwable> throwableRef = new AtomicReference<>();
    final CountDownLatch latch = new CountDownLatch(1);

    // If streaming were broken, the call to enqueue would throw the exception synchronously.
    call.enqueue(
        new Callback<Void>() {
          @Override
          public void onResponse(Call<Void> call, Response<Void> response) {
            latch.countDown();
          }

          @Override
          public void onFailure(Call<Void> call, Throwable t) {
            throwableRef.set(t);
            latch.countDown();
          }
        });
    latch.await();

    Throwable throwable = throwableRef.get();
    assertThat(throwable).isInstanceOf(EOFException.class);
    assertThat(throwable).hasMessageThat().isEqualTo("oops!");
  }
}

package elasticsearchtest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.hasSize;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.http.ssl.SSLContextBuilder;
import org.junit.jupiter.api.Test;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.json.jackson.JacksonJsonpMapper;
import org.opensearch.client.opensearch.OpenSearchClient;
import org.opensearch.client.opensearch._types.Refresh;
import org.opensearch.client.opensearch._types.Result;
import org.opensearch.client.opensearch.core.IndexRequest;
import org.opensearch.client.opensearch.indices.CreateIndexRequest;
import org.opensearch.client.opensearch.indices.DeleteIndexRequest;
import org.opensearch.client.opensearch.indices.IndexSettings;
import org.opensearch.client.opensearch.indices.PutIndicesSettingsRequest;
import org.opensearch.client.transport.rest_client.RestClientTransport;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

@Testcontainers
class ElasticsearchTest {

  @Container
  private static final ElasticsearchContainer ES =
    new ElasticsearchContainer("docker.elastic.co/elasticsearch/elasticsearch:8.3.3");

  @Test
  void test() throws Exception {
    // Create the OpenSearchClient
    var credentialsProvider = new BasicCredentialsProvider();
    credentialsProvider.setCredentials(AuthScope.ANY,
      new UsernamePasswordCredentials("elastic", ElasticsearchContainer.ELASTICSEARCH_DEFAULT_PASSWORD));
    var sslContext = SSLContextBuilder.create().loadTrustMaterial(null, (chains, authType) -> true).build();
    var restClient = RestClient.builder(new HttpHost(ES.getHost(), ES.getFirstMappedPort(), "https")).
        setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
          @Override
          public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
            return httpClientBuilder
                .setDefaultCredentialsProvider(credentialsProvider)
                .setSSLHostnameVerifier(NoopHostnameVerifier.INSTANCE)
                .setSSLContext(sslContext);
          }
        }).build();
    var transport = new RestClientTransport(restClient, new JacksonJsonpMapper());
    var client = new OpenSearchClient(transport);

    // Create the index
    var index = "sample-index";
    var createIndexRequest = new CreateIndexRequest.Builder().index(index).build();
    var createIndexResponse = client.indices().create(createIndexRequest);
    assertThat(createIndexResponse.acknowledged(), is(true));

    // Configure the index
    var indexSettings = new IndexSettings.Builder().autoExpandReplicas("0-all").build();
    var putIndicesSettingsRequest = new PutIndicesSettingsRequest.Builder().index(index).settings(indexSettings).build();
    var putIndicesSettingsResponse = client.indices().putSettings(putIndicesSettingsRequest);
    assertThat(putIndicesSettingsResponse.acknowledged(), is(true));

    // Index a Person
    var indexData = new Person("Morning", "Glory");
    var indexRequest = new IndexRequest.Builder<Person>().index(index).id("1").document(indexData)
        .refresh(Refresh.WaitFor).build();
    var indexResponse = client.index(indexRequest);
    assertThat(indexResponse.result(), is(Result.Created));

    // Search for the Person
    var searchResponse = client.search(s -> s.index(index), Person.class);
    assertThat(searchResponse.hits().hits(), hasSize(1));

    // Delete the Person
    var deleteResponse = client.delete(b -> b.index(index).id("1"));
    assertThat(deleteResponse.result(), is(Result.Deleted));

    // Delete the index
    var deleteIndexRequest = new DeleteIndexRequest.Builder().index(index).build();
    var deleteIndexResponse = client.indices().delete(deleteIndexRequest);
    assertThat(deleteIndexResponse.acknowledged(), is(true));
  }

  static class Person {
    private String firstName;
    private String lastName;

    public Person() {
    }

    public Person(String firstName, String lastName) {
      this.firstName = firstName;
      this.lastName = lastName;
    }

    public String getFirstName() {
      return firstName;
    }

    public void setFirstName(String firstName) {
      this.firstName = firstName;
    }

    public String getLastName() {
      return lastName;
    }

    public void setLastName(String lastName) {
      this.lastName = lastName;
    }
  }

}

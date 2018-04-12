package org.elastic;

import static org.elasticsearch.common.settings.ImmutableSettings.settingsBuilder;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.elastic.doc.Documento;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingResponse;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.action.admin.indices.recovery.RecoveryRequest;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequestBuilder;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.elasticsearch.search.SearchHit;

public class ElasticSeachUtil {

	public static String INDEX_NAME = "ind_na1m11";
	public static String CLUSTER_NAME = "cluster_name1";
	public static Client client;
	public static Node node;

	public static void elasticIni() throws IOException, InterruptedException {

		System.out.println("Inicializando...");
		System.out.println(INDEX_NAME);
		System.out.println(CLUSTER_NAME);
		Settings settings = settingsBuilder().put("script.disable_dynamic", false).build();

		/**
		 * The Java client of Elasticsearch allows to embed an engine of this
		 * kind by creating an in-memory node. This can be done using the class
		 * NodeBuilder
		 */
		node = NodeBuilder.nodeBuilder().settings(settings).clusterName(CLUSTER_NAME).node();

		/**
		 * Getting a client instance to interact with this node is simple thanks
		 * to the method client of the class Node.
		 */
		client = node.client();

		// .field("type", "stemmer_override").field("rules_path",
		// "B:/estudo/java/forge-distribution-3.4.0.Final/bin/smartsearch/src/main/resources/rules_path.txt").endObject()

		/**
		 * This code uses the default configuration of Elasticsearch. We can
		 * override configuration entities by using the method setSettings of
		 * the class NodeBuilder. You can notice that a folder data in the
		 * current directory to store data. This folder isnt deleted after the
		 * node shuts down. If we want to Clearing data, we can remove it.
		 */
		Settings indexSettings = settingsBuilder().loadFromSource(jsonBuilder().startObject()
				// Add analyzer settings
				.startObject("analysis").startObject("filter").startObject("portuguese_stop").field("type", "stop")
				.field("stopwords_path", "B:/estudo/java/forge-distribution-3.4.0.Final/bin/elastic/src/main/resources/stopwords.txt")
				.endObject().startObject("portuguese_stemmer").field("type", "stemmer")
				.field("name", "light_portuguese").endObject().startObject("test_filter_synonyms_pt")
				.field("type", "synonym")
				.field("synonyms_path",
						"B:/estudo/java/forge-distribution-3.4.0.Final/bin/elastic/src/main/resources/synonym.txt")
				.field("ignore_case", true).field("expand", true).endObject().startObject("test_filter_ngram")
				.field("type", "edgeNGram").field("min_gram", 1).field("max_gram", 80).endObject().startObject("lowercase")
				.field("type", "lowercase").endObject().endObject()
				.startObject("analyzer").startObject("test_analyzer").field("type", "custom")
				.field("tokenizer", "whitespace")
				.field("filter",
						new String[] { "lowercase", "test_filter_synonyms_pt", "portuguese_stop",
								"portuguese_stemmer" })
				.endObject().endObject().endObject().endObject().string()).build();

		/**
		 * ElasticSearch provides a create index request to create an index on a
		 * node, as described below:
		 */
		CreateIndexRequest indexRequest = new CreateIndexRequest(INDEX_NAME, indexSettings);
	
		client.admin().indices().create(indexRequest).actionGet();

		/**
		 * Parameter store. This specifies if the field must be stored or not in
		 * the index. The possible values are true or false. Parameter index.
		 * This specifies what must be done during the index phasis (the field
		 * must be analyzed or not). The possible values are analyzed and
		 * not_analyzed. The parameter analyzer specifies the analyzer to use
		 * for both indexing and search. Parameters index_analyzer and
		 * search_analyzer can be used to define different analyzers for
		 * indexing and search.
		 */
		String mapping = "{" + "\"doc\"" + ":" + "{" + "\"properties\": " + "{" + "\"id\"" + ":" + "{" + "\"type\""
				+ ":" + "\"string\"" + "," + "\"store\"" + " :" + true + "}," + "\"valor\"" + ": {" + "\"type\"" + ":"
				+ "\"string\"" + "," + "\"search_analyzer\"" + ":" + "\"test_analyzer\"" + "," + "\"index_analyzer\"" + ":"
				+ "\"test_analyzer\"" + "," + "\"store\"" + " :" + true + "}," + "\"conteudo\"" + ": {" +

				"\"search_analyzer\"" + " :" + "\"test_analyzer\"" + "," + "\"index_analyzer\"" + " :" + "\"standard\""
				+ "," + "\"type\"" + ":" + "\"string\"" + "," + "\"store\"" + " :" + true + "}," + "\"tags\"" + ":"
				+ "{" + "\"type\"" + ":" + "\"string\"" + "," + "\"search_analyzer\"" + ":" + "\"test_analyzer\"" + ","
				+ "\"index_analyzer\"" + ":" + "\"standard\"" + "," + "\"store\"" + " :" + true + "}" +

				"}" +

				"}" + "}";
		/**
		 * Now the index is created, we can create types in it. A best practice
		 * consists in specifying the mapping for types.
		 */
		System.out.println(mapping);
		PutMappingResponse response = client.admin().indices().preparePutMapping(INDEX_NAME).setSource(mapping)
				.setType("doc").execute().actionGet();
	}
	
	
    /**
     * 
     * @throws IOException
     */
	public static void elasticClose() throws IOException {

		System.out.println("Finalizando...");
		DeleteIndexRequest dir = new DeleteIndexRequest(INDEX_NAME);
		client.admin().indices().delete(dir).actionGet();
		client.close();
		node.close();

	}
	
	
	/**
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param id
	 */
	public static void getDocument(Client client, String index, String type, String id) {

		GetResponse getResponse = client.prepareGet(index, type, id).execute().actionGet();
		Map<String, Object> source = getResponse.getSource();

		System.out.println("------------------------------");
		System.out.println("Index: " + getResponse.getIndex());
		System.out.println("Type: " + getResponse.getType());
		System.out.println("Id: " + getResponse.getId());
		System.out.println("Version: " + getResponse.getVersion());
		System.out.println(source);
		System.out.println("------------------------------");

	}

	/**
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param field
	 * @param value
	 * Busca no campo conteudo
	 */
	public static void searchDocument(Client client, String index, String type, String field, String value) {

		SearchResponse response = client.prepareSearch(index).setTypes(type).setSearchType(SearchType.QUERY_AND_FETCH)
				.setQuery(QueryBuilders.matchQuery(field, value)).setFrom(0).setSize(60).setExplain(true).execute()
				.actionGet();

		

		SearchHit[] results = response.getHits().getHits();

		System.out.println("Current results: " + results.length);
		for (SearchHit hit : results) {
			System.out.println("------------------------------");
			Map<String, Object> result = hit.getSource();
			System.out.println(result.get("conteudo").toString());
		}
	}
	
	
	/**
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param field1
	 * @param value1
	 * @param field2
	 * @param value2
	 * Busca no campo conteudo ou tags
	 */
	public static void searchDocument(Client client, String index, String type, String field1, String value1, String field2, String value2) {
        //pesquisa por até três tags
		String[] postTags = value2.split(",");
	    String first = postTags[0].isEmpty()?"":postTags[0];
	    String second = postTags[0].isEmpty()?"":postTags[0];
	    String third = postTags[0].isEmpty()?"":postTags[0];
		SearchResponse response = client.prepareSearch(index).setTypes(type)
				.setQuery(QueryBuilders.boolQuery()
		                   .should(QueryBuilders.matchPhraseQuery(field1, value1))
		                   .should(QueryBuilders.termsQuery(field2, first, second, third))).execute().actionGet();
		

		SearchHit[] results = response.getHits().getHits();

		System.out.println("Current results: " + results.length);
		for (SearchHit hit : results) {
			System.out.println("------------------------------");
			Map<String, Object> result = hit.getSource();
			System.out.println(result.get("conteudo").toString());
			System.out.println(result.get("tags").toString());
		}
	}
	
	
	/**
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param value
	 * Busca em todos os campos
	 */
	public static void searchDocument(Client client, String index, String type, String value) {

		QueryBuilder queryBuilder = QueryBuilders.queryString("*" + value + "*");

		SearchRequestBuilder searchRequestBuilder = client.prepareSearch(index);
		searchRequestBuilder.setTypes(type);
		searchRequestBuilder.setSearchType(SearchType.DEFAULT);
		searchRequestBuilder.setQuery(queryBuilder);
		searchRequestBuilder.setFrom(0).setSize(60).setExplain(true);

		SearchResponse response = searchRequestBuilder.execute().actionGet();
		
		SearchHit[] results = response.getHits().getHits();

		System.out.println("Current results: " + results.length);
		for (SearchHit hit : results) {
			System.out.println("------------------------------");
			Map<String, Object> result = hit.getSource();
			System.out.println(result);
		}
	}

	
	/**
	 * 
	 * @param client
	 * @param type
	 * @param cod
	 * @param index
	 * @param id
	 * @param valor
	 * @param conteudo
	 * @param tags
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 *             The initialization of data sets can leverage the bulk API of
	 *             ElasticSearch. This allows to send several data to add in a
	 *             single call.
	 */
	public static void addDocumentBulk(Client client, String type, String cod, String index, Documento doc)
			throws IOException, InterruptedException, ExecutionException {

		BulkRequestBuilder bulkRequest = client.prepareBulk();
		String[] postTags = doc.getTags().split(",");
		// prepare json builder
		XContentBuilder builder1 = XContentFactory.jsonBuilder().startObject().field("id", doc.getId())
				.field("valor", doc.getValor()).field("conteudo", doc.getConteudo()).field("tags", postTags)
				.endObject();

		// prepare index request
		IndexRequestBuilder irb1 = client.prepareIndex(index, type, cod).setSource(builder1);

		bulkRequest.add(irb1);

		BulkResponse bulkResponse = bulkRequest.execute().actionGet();
		if (bulkResponse.hasFailures()) {
			// process failures by iterating through each bulk response item
			System.err.println("Has failures");
		} else {
			System.out.println("Bulk OK !");
			client.admin().indices().prepareRefresh(INDEX_NAME).execute().actionGet();
		}
	}

	/**
	 * 
	 * @param sinonimo
	 * @param client
	 * @param index
	 * @throws IOException
	 * @throws InterruptedException
	 * @throws ExecutionException
	 * Adiciona novos sinônimos no arquivo
	 */
	public static void addSynonym(String sinonimo, Client client, String index)
			throws IOException, InterruptedException, ExecutionException {

		// close the index before the update
		//client.admin().indices().close(new CloseIndexRequest(index));
		 CloseIndexResponse closeIndexResponse =
		 client.admin().indices().close(Requests.closeIndexRequest(index)).actionGet();

		BufferedWriter bw = null;

		try {
			bw = new BufferedWriter(new FileWriter(
					"B:/estudo/java/forge-distribution-3.4.0.Final/bin/smartsearch/src/main/resources/synonym.txt",
					true));
			bw.newLine();
			bw.write(sinonimo);
			bw.flush();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		} finally { // always close the file
			if (bw != null) {
				try {
					bw.close();

				} catch (IOException ioe2) {
					// just ignore it
				}
			}
		}
		System.out.println("Atualizando index...");
		
		Settings indexSettings = settingsBuilder().loadFromSource(jsonBuilder().startObject()
				// Add analyzer settings
				.startObject("analysis").startObject("filter").startObject("portuguese_stop").field("type", "stop")
				.field("stopwords_path", "B:/estudo/java/forge-distribution-3.4.0.Final/bin/elastic/src/main/resources/stopwords.txt")
				.endObject().startObject("portuguese_stemmer").field("type", "stemmer")
				.field("name", "light_portuguese").endObject().startObject("test_filter_synonyms_pt")
				.field("type", "synonym")
				.field("synonyms_path",
						"B:/estudo/java/forge-distribution-3.4.0.Final/bin/elastic/src/main/resources/synonym.txt")
				.field("ignore_case", true).field("expand", true).endObject().startObject("test_filter_ngram")
				.field("type", "edgeNGram").field("min_gram", 1).field("max_gram", 80).endObject().startObject("lowercase")
				.field("type", "lowercase").endObject().endObject()
				.startObject("analyzer").startObject("test_analyzer").field("type", "custom")
				.field("tokenizer", "whitespace")
				.field("filter",
						new String[] { "lowercase", "test_filter_synonyms_pt", "portuguese_stop",
								"portuguese_stemmer" })
				.endObject().endObject().endObject().endObject().string()).build();
		// update the synonyms
		client.admin().indices().prepareUpdateSettings(index).setSettings(indexSettings).get();

		// open the index

		OpenIndexResponse openIndexResponse = client.admin().indices().open(new OpenIndexRequest(index)).get();

		client.admin().indices().recoveries(new RecoveryRequest(index)).actionGet();
	}
	
	/**
	 * 
	 * @param client
	 * @param index
	 * @param type
	 * @param id
	 */
	 public static void deleteDocument(Client client, String index, String type, String id){
	        
	        DeleteResponse response = client.prepareDelete(index, type, id).execute().actionGet();
	        client.admin().indices().prepareRefresh(INDEX_NAME).execute().actionGet();
	        System.out.println("\nDocumento deletado");
	        System.out.println("Index: " + response.getIndex());
	        System.out.println("Type: " + response.getType());
	        System.out.println("Id: " + response.getId());
	        System.out.println("Version: " + response.getVersion());
	        
	    }

}

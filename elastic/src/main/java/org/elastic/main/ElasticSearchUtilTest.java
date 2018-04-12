package org.elastic.main;

import java.io.IOException;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.elastic.ElasticSeachUtil;
import org.elastic.doc.Documento;

public class ElasticSearchUtilTest {

	public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
		

		Documento doc = new Documento("1", "teste",
				"Lorem Ipsum é simplesmente uma simulação de texto da indústria tipográfica e de impressos,"
						+ " e vem sendo utilizado desde o século XVI, quando um impressor desconhecido pegou uma bandeja de tipos e os embaralhou para fazer"
						+ " um livro de modelos de tipos. Lorem Ipsum sobreviveu não só a cinco séculos, como também ao salto para a editoração eletrônica, "
						+ "permanecendo essencialmente inalterado. Se popularizou na década de 60, quando a Letraset lançou decalques contendo passagens de Lorem Ipsum,"
						+ " e mais recentemente quando passou a ser integrado a softwares de editoração eletrônica como Aldus PageMaker.",
				"lorem, ipsum, texto, tipografia");

		Documento doc1 = new Documento("2", "teste2",
				"É um fato conhecido de todos que um universidade fe  universidade federal do maranhão bacias hidrográficas se distrairá com o conteúdo de texto legível de AIA uma página quando"
						+ " estiver examinando sua diagramação. A vantagem de usar  que ele tem uma distribuição normal de letras, ao contrário de Conteúdo aqui, "
						+ "conteúdo aqui, fazendo com que ele mostre uma aparência similar a de um texto legível. ",
				"lorem, ipsum, gato");
		
		
		ElasticSeachUtil.elasticIni();
		ElasticSeachUtil.addSynonym("mostre, mostrar, expor, apresentar", ElasticSeachUtil.client, ElasticSeachUtil.INDEX_NAME); 
		ElasticSeachUtil.addSynonym("ufma, universidade federal do maranhão", ElasticSeachUtil.client, ElasticSeachUtil.INDEX_NAME); 
		ElasticSeachUtil.addDocumentBulk(ElasticSeachUtil.client, "doc", UUID.randomUUID().toString(), ElasticSeachUtil.INDEX_NAME, doc1);
		ElasticSeachUtil.addDocumentBulk(ElasticSeachUtil.client, "doc",  UUID.randomUUID().toString(), ElasticSeachUtil.INDEX_NAME, doc);
		ElasticSeachUtil.searchDocument(ElasticSeachUtil.client, ElasticSeachUtil.INDEX_NAME, "doc",  "conteudo", "avaliação de impacte ambiental","tags","textoA, tipografias");//
		ElasticSeachUtil.getDocument(ElasticSeachUtil.client, ElasticSeachUtil.INDEX_NAME, "doc", "1");
		ElasticSeachUtil.searchDocument(ElasticSeachUtil.client, ElasticSeachUtil.INDEX_NAME, "doc", "conteudo", "lorem ipsum");	//	
		//ElasticSeachUtil.deleteDocument(ElasticSeachUtil.client,  ElasticSeachUtil.INDEX_NAME, "doc", "1");
	
		ElasticSeachUtil.getDocument(ElasticSeachUtil.client, ElasticSeachUtil.INDEX_NAME, "doc", "1");
		ElasticSeachUtil.searchDocument(ElasticSeachUtil.client, ElasticSeachUtil.INDEX_NAME, "doc", "ufma");//
		ElasticSeachUtil.elasticClose();;
		
		
	}

}

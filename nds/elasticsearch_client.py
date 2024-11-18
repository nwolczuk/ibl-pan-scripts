from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer
import os

username = 'elastic'
# password = os.getenv('ELASTIC_PASSWORD') # Value you set in the environment variable
password = 'estest'

client = Elasticsearch(
    "http://localhost:9200",
    basic_auth=(username, password)
)

print(client.info())

# Delete the index
client.indices.delete(index="article_index", ignore_unavailable=True)

# Define the mapping
mappings = {
    "properties": {
        'title': {
            'type': 'text',
            'analyzer': 'polish'
            },
        'content': {
            'type': 'text',
            'analyzer': 'polish'
            },
        "vector_title": {
            "type": "dense_vector",
            "dims": 768  
            },
          "vector_content": {
            "type": "dense_vector",
            "dims": 768  
            },
        }
}

client.indices.exists(index='article_index')
# Create the index
client.indices.create(index="article_index", mappings=mappings)

article_1 = {
    'title': 'Artykuł o mrówkach',
    'content': 'Mrówki mają dużo nóg i są bardzo pracowite.'
    }

article_2 = {
    'title': 'Artykuł o lodowcach',
    'content': 'Lodowce są bardzo zimne. Nie lubimy takich zimnych klimatów.'
    }

article_3 = {
    'title': 'Niewiedza jest złem',
    'content': 'W starożytnych czasach ludzie bali się niewiedzy. I mieli rację. Głupota zazwyczaj prowadziła do przedwczesnego zgonu.'
    }

client.index(index='article_index', body=article_1)
client.index(index='article_index', body=article_2)

# pretty print response
def pretty_response(response):
    if len(response["hits"]["hits"]) == 0:
        print("Your search returned no results.")
    else:
        for hit in response["hits"]["hits"]:
            id = hit["_id"]
            score = hit["_score"]
            title = hit["_source"]["title"]
            content = hit["_source"]["content"]
            pretty_output = f"\nID: {id}\nTitle: {title}\nContent: {content}\nScore: {score}"
            print(pretty_output)


search_term = 'pracowite'
response = client.search(
    index="article_index", query={"multi_match": {
            "query": search_term,
            "fields": ["title", "content"]
        }}
)

pretty_response(response)


# SEMANTIC SEARCH
# Inicjalizacja modelu Sentence-BERT
model = SentenceTransformer('paraphrase-xlm-r-multilingual-v1')

# Funkcja do generowania wektora dla tekstu
def get_vector(text):
    return model.encode(text)

# Funkcja do dodawania dokumentów do indeksu
def index_document(title, content):
    vector_title = get_vector(title).tolist()
    vector_content = get_vector(content).tolist()
    client.index(index='article_index', body={
        'title': title,
        'content': content,
        'vector_title': vector_title,
        'vector_content': vector_content
    })

index_document(**article_1)
index_document(**article_2)
index_document(**article_3)

# Funkcja do wyszukiwania podobieństwa semantycznego
def search_semantic(query, size, min_score=0.0):
    vector_query = get_vector(query).tolist()
    search_query = {
        "size": size,
        'min_score': min_score,
        "query": {
            "function_score": {
                "functions": [
                    {
                        "script_score": {
                            "script": {
                                "source": "cosineSimilarity(params.query_vector, 'vector_content') + cosineSimilarity(params.query_vector, 'vector_title')",
                                "params": {
                                    "query_vector": vector_query
                                }
                            }
                        }
                    }
                ],
                "boost_mode": "replace"
            }
        }
    }
    response = client.search(index='article_index', body=search_query)
    return response

search_query = 'lód'
results = search_semantic(search_query, 3)
pretty_response(results)











def index_document(doc_id, text):
    client.index(index='pdf-index', id=doc_id, body={'text': text})
    
def search_documents(query):
    response = client.search(index='pdf-index', body={
        'query': {
            'match': {
                'text': query
            }
        }
    })
    return response['hits']['hits']

# Przykład wyszukiwania
results = search_documents('example query')
for result in results:
    print(result['_source']['text'])
from elasticsearch import Elasticsearch
from sentence_transformers import SentenceTransformer
import os

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


username = 'elastic'
# password = os.getenv('ELASTIC_PASSWORD') # Value you set in the environment variable
password = ''

client = Elasticsearch(
    "https://localhost:9200",
    basic_auth=(username, password),
    verify_certs=False,
)

print(client.info())

index_body = {
    "mappings": {
        "properties": {
            "title": {
                "type": "text",
                "analyzer": "polish"
            },
            "content": {
                "type": "text",
                "analyzer": "polish"
            },
        }
    }
}

# Usuń indeks, jeśli istnieje
if client.indices.exists(index='article_index'):
    client.indices.delete(index='article_index')

client.indices.create(index='article_index', body=index_body)

article_1 = {
    'title': 'Pamięć, wyobraźnia, praktyki oporu',
    'content': 'Świat to skomplikowane miejsce, a my jesteśmy w gruncie rzeczy dość prości. Jak proste istoty radzą sobie z narastającą złożonością świata? Oto pytanie, które stawiamy sobie, prowadząc badania nad kulturą wernakularną. Obszarem zawartych w tym tomie analiz jest właśnie przestrzeń rozciągająca się pomiędzy złożonością współczesnej kultury (relacji społecznych, ekonomii, polityki itd.) a prostotą narzędzi, którymi dysponujemy, by urządzić się w niej i działać sprawnie. Interesowały nas procesy upraszczania rzeczywistości, w ramach których jednostki ją postrzegają i opisują, planują i podejmują działania, a także budują swoją tożsamość i synchronizują wysiłki w większych grupach. Kultura wernakularna, jaką opisujemy w tej książce, stanowi próbę radzenia sobie prostymi środkami w skomplikowanym świecie.'
    }

article_2 = {
    'title': 'Rola programów rządowych w zwiększaniu liczebności Sił Zbrojnych RP',
    'content': 'Zmieniające się wyzwania dotyczące bezpieczeństwa państwa, zmiana struktury demograficznej społeczeństwa powodują konieczność rozszerzenia i intensyfikacji działań zapewniających Polsce skuteczną obronę i odstraszanie. Polska na przestrzeni lat prowadziła programy mające na celu wpływanie na liczebność i jakość Sił Zbrojnych RP jako jednego z elementów bezpieczeństwa państwa. W artykule przedstawiono zależność pomiędzy programami aktywizacji społeczeństwa a dynamiką wzrostu liczebności Sił Zbrojnych RP. Pojawiające się analizy postrzegania przez Polaków zaangażowania w obronę ojczyzny wskazują na zasadność wprowadzania nowych programów, które mogą wpływać na zmiany postaw społecznych, skutkujące wzrostem liczebności Sił Zbrojnych RP.'
    }

article_3 = {
    'title': 'Placówki ochrony zdrowia oraz kadra lekarska w Częstochowie i powiecie częstochowskim w latach 1918-1939',
    'content': 'Monografia Juliusza Sętowskiego Placówki ochrony zdrowia oraz kadra lekarska w Częstochowie  i powiecie częstochowskim w latach 1918-1939 w sposób kompleksowy omawia funkcjonowanie służby zdrowia w czasach II Rzeczypospolitej. Należy podkreślić, że do tej pory brak było tak całościowego spojrzenia na te kwestie, co czyni tę pozycję wyjątkowo interesującą i zapełniającą lukę na rynku wydawniczym. Autor omówił powstanie, funkcjonowanie i świadczone usługi dla ludności szpitali miejskich, tj. Szpitala Najświętszej Maryi Panny, Szpitala Miejskiego dla Chorych Zakaźnych, Szpitala Miejskiego dla Chorych Wewnętrznych oraz Szpitala dla Chorych Kobiet Wenerycznie, a także Szpitala Powszechnego Miejskiego, Szpitala Towarzystwa Dobroczynności dla Żydów, Szpitala Powiatowej Kasy Chorych. Zasygnalizowano też rolę i znaczenie funkcjonowania szpitalnictwa wojskowego, tak istotnego podczas powstań śląskich oraz wojny polsko-bolszewickiej. Autor pokusił się także o przedstawienie funkcjonowania placówek zdrowia – szpitali, przychodni i ośrodków zdrowia - w miejscowościach powiatu częstochowskiego, podkreślając rolę i znaczenie tamtejszego personelu  w podnoszeniu stanu zdrowotnego ludności. Istotną część monografii stanowią biogramy lekarek i lekarzy  Częstochowy i powiatu częstochowskiego. Biogramy przedstawiają jednostkowe losy tej grupy zawodowej. Na podkreślenie zasługuje bogata bibliografia, zawierająca zwłaszcza wybór źródeł z czasów dwudziestolecia międzywojennego.'
    }

client.index(index='article_index', body=article_1)
client.index(index='article_index', body=article_2)
client.index(index='article_index', body=article_3)

search_term = 'kasa chorych'

query = {
    "query": {
        "multi_match": {
            "query": search_term,
            "fields": ["title", "content"]
        }
    }
}

results = client.search(index='article_index', body=query)
pretty_response(results)



# SEMANTIC SEARCH
article_1 = {
    'title': 'Pamięć, wyobraźnia, praktyki oporu',
    'content': 'Świat to skomplikowane miejsce, a my jesteśmy w gruncie rzeczy dość prości. Jak proste istoty radzą sobie z narastającą złożonością świata? Oto pytanie, które stawiamy sobie, prowadząc badania nad kulturą wernakularną. Obszarem zawartych w tym tomie analiz jest właśnie przestrzeń rozciągająca się pomiędzy złożonością współczesnej kultury (relacji społecznych, ekonomii, polityki itd.) a prostotą narzędzi, którymi dysponujemy, by urządzić się w niej i działać sprawnie. Interesowały nas procesy upraszczania rzeczywistości, w ramach których jednostki ją postrzegają i opisują, planują i podejmują działania, a także budują swoją tożsamość i synchronizują wysiłki w większych grupach. Kultura wernakularna, jaką opisujemy w tej książce, stanowi próbę radzenia sobie prostymi środkami w skomplikowanym świecie.'
    }

article_2 = {
    'title': 'Rola programów rządowych w zwiększaniu liczebności Sił Zbrojnych RP',
    'content': 'Zmieniające się wyzwania dotyczące bezpieczeństwa państwa, zmiana struktury demograficznej społeczeństwa powodują konieczność rozszerzenia i intensyfikacji działań zapewniających Polsce skuteczną obronę i odstraszanie. Polska na przestrzeni lat prowadziła programy mające na celu wpływanie na liczebność i jakość Sił Zbrojnych RP jako jednego z elementów bezpieczeństwa państwa. W artykule przedstawiono zależność pomiędzy programami aktywizacji społeczeństwa a dynamiką wzrostu liczebności Sił Zbrojnych RP. Pojawiające się analizy postrzegania przez Polaków zaangażowania w obronę ojczyzny wskazują na zasadność wprowadzania nowych programów, które mogą wpływać na zmiany postaw społecznych, skutkujące wzrostem liczebności Sił Zbrojnych RP.'
    }

article_3 = {
    'title': 'Placówki ochrony zdrowia oraz kadra lekarska w Częstochowie i powiecie częstochowskim w latach 1918-1939',
    'content': 'Monografia Juliusza Sętowskiego Placówki ochrony zdrowia oraz kadra lekarska w Częstochowie  i powiecie częstochowskim w latach 1918-1939 w sposób kompleksowy omawia funkcjonowanie służby zdrowia w czasach II Rzeczypospolitej. Należy podkreślić, że do tej pory brak było tak całościowego spojrzenia na te kwestie, co czyni tę pozycję wyjątkowo interesującą i zapełniającą lukę na rynku wydawniczym. Autor omówił powstanie, funkcjonowanie i świadczone usługi dla ludności szpitali miejskich, tj. Szpitala Najświętszej Maryi Panny, Szpitala Miejskiego dla Chorych Zakaźnych, Szpitala Miejskiego dla Chorych Wewnętrznych oraz Szpitala dla Chorych Kobiet Wenerycznie, a także Szpitala Powszechnego Miejskiego, Szpitala Towarzystwa Dobroczynności dla Żydów, Szpitala Powiatowej Kasy Chorych. Zasygnalizowano też rolę i znaczenie funkcjonowania szpitalnictwa wojskowego, tak istotnego podczas powstań śląskich oraz wojny polsko-bolszewickiej. Autor pokusił się także o przedstawienie funkcjonowania placówek zdrowia – szpitali, przychodni i ośrodków zdrowia - w miejscowościach powiatu częstochowskiego, podkreślając rolę i znaczenie tamtejszego personelu  w podnoszeniu stanu zdrowotnego ludności. Istotną część monografii stanowią biogramy lekarek i lekarzy  Częstochowy i powiatu częstochowskiego. Biogramy przedstawiają jednostkowe losy tej grupy zawodowej. Na podkreślenie zasługuje bogata bibliografia, zawierająca zwłaszcza wybór źródeł z czasów dwudziestolecia międzywojennego.'
    }

# delet index if exists
if client.indices.exists(index='article_index'):
    client.indices.delete(index='article_index')


index_document(**article_1)
index_document(**article_2)
index_document(**article_3)

# cosine search
search_query = 'leczenie'
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
import os
import pandas as pd
from neo4j import GraphDatabase

senha = "xBM5FUGtBJt-_TYGhK3mEjYOyeQAg5LZi46au-_nsK0"
url = "neo4j+s://044ec74b.databases.neo4j.io"
usuario = "neo4j"

from posixpath import join

class CriarPokemons:
  def __init__(self, uri, user, password):
    self.driver = GraphDatabase.driver(uri, auth=(user, password))

  def close(self):
    self.driver.close()

  def carrega_base(self, csv_path):
    # Lê o arquivo CSV
    pokemons_df = pd.read_csv(csv_path)
    with self.driver.session() as session:
      session.write_transaction(self._zera_base)
      session.write_transaction(self._cria_pokemons, pokemons_df)

  @staticmethod
  def _zera_base(tx):
    tx.run("""
    MATCH (n)
    DETACH DELETE n
    """)

  @staticmethod
  def _cria_pokemons(tx, pokemons_df):
    # Primeira etapa: criar todos os nós de Pokémon
    for index, row in pokemons_df.iterrows():
      # Cria o nó do Pokémon
      tx.run("""
      CREATE (p:Pokemon {nome: $nome, numero: $numero, peso: $peso, altura: $altura, url_pokemon: $url_pokemon})
      """, nome=row['Nome'], numero=row['Número'], peso=row['Peso'], altura=row['Altura'], url_pokemon=row['Url Pokémon'])

      # Pega as habilidades e suas descrições
      habilidades = row['Habilidades'].split(',')
      descricoes = row['Habilidades Descrição'].split(',')

      # Para cada habilidade, cria o nó e o relacionamento
      for habilidade, descricao in zip(habilidades, descricoes):
        # Cria o nó de habilidade, caso ainda não exista
        tx.run("""
        MERGE (h:Habilidade {nome: $habilidade, descricao: $descricao})
        """, habilidade=habilidade.strip(), descricao=descricao.strip())

        # Relaciona o Pokémon à habilidade
        tx.run("""
        MATCH (p:Pokemon {nome: $nome}), (h:Habilidade {nome: $habilidade})
        CREATE (p)-[:TEM_HABILIDADE]->(h)
        """, nome=row['Nome'], habilidade=habilidade.strip())

      # Pega os tipos e cria os relacionamentos
      tipos = row['Tipos'].split(',')
      for tipo in tipos:
        # Cria o nó de tipo, caso ainda não exista
        tx.run("""
        MERGE (t:Tipo {nome: $tipo})
        """, tipo=tipo.strip())

        # Relaciona o Pokémon ao tipo
        tx.run("""
        MATCH (p:Pokemon {nome: $nome}), (t:Tipo {nome: $tipo})
        CREATE (p)-[:TEM_TIPO]->(t)
        """, nome=row['Nome'], tipo=tipo.strip())

    # Segunda etapa: criar todas as relações de evolução
    for index, row in pokemons_df.iterrows():
      # Pega as evoluções, números e URLs (com verificação de valores NaN)
      evolucoes = row['Evoluções'].split(',') if isinstance(row['Evoluções'], str) else []
      evolucoes_numeros = row['Evoluções Número'].split(',') if isinstance(row['Evoluções Número'], str) else []
      evolucoes_urls = row['Evoluções URL'].split(',') if isinstance(row['Evoluções URL'], str) else []

      # Se houver evoluções, crie a cadeia de evoluções corretamente
      pokemon_atual = row['Nome']
      for evolucao, numero, url in zip(evolucoes, evolucoes_numeros, evolucoes_urls):
        evolucao = evolucao.strip()
        numero = numero.strip() if numero else None
        url = url.strip() if url else None

        # Cria a relação de evolução
        tx.run("""
        MATCH (p1:Pokemon {nome: $pokemon_atual}), (p2:Pokemon {nome: $evolucao})
        MERGE (p1)-[:EVOLUI_PARA]->(p2)
        """, pokemon_atual=pokemon_atual, evolucao=evolucao)

        # Atualiza o Pokémon atual para a próxima evolução
        pokemon_atual = evolucao


#Consultas
class Neo4jConnection:
  def __init__(self, uri, user, pwd):
    self.__uri = uri
    self.__user = user
    self.__pwd = pwd
    self.__driver = None
    try:
        self.__driver = GraphDatabase.driver(self.__uri, auth=(self.__user, self.__pwd))
    except Exception as e:
        print("Failed to create the driver:", e)

  def close(self):
    if self.__driver is not None:
        self.__driver.close()

  def query(self, query, parameters=None, db=None):
    assert self.__driver is not None, "Driver not initialized!"
    session = None
    response = None
    try:
      session = self.__driver.session(database=db) if db is not None else self.__driver.session()
      response = list(session.run(query, parameters))
    except Exception as e:
      print("Query failed:", e)
    finally:
      if session is not None:
        session.close()
    return response

conn = Neo4jConnection(uri=url,
                       user=usuario,
                       pwd=senha)

def consultar(query):
  return conn.query(query)
  # for line in conn.query(query):
  #   print(dict(line))

criador_pokemons = CriarPokemons(url, usuario, senha)
csv_path = "/content/pokemons.csv"
criador_pokemons.carrega_base(csv_path)
criador_pokemons.close()

print("Quais Pokémons podem atacar um Pikachu pelo sua fraqueza (Ground) cujo o peso é mais de 10kg:\n")

consultar("""
MATCH (p:Pokemon)-[:TEM_TIPO]->(t:Tipo)
WHERE t.nome = 'Ground' 
  AND toFloat(replace(p.peso, ' kg', '')) > 10
RETURN p.nome AS Nome, 
       t.nome AS Tipo, 
       p.peso AS Peso
ORDER BY p.nome;
""")


print("Número de Evoluções para Dobrar Peso do Pokémon:\n")

consultar("""
MATCH path=(p1:Pokemon)-[:EVOLUI_PARA*1..]->(p2:Pokemon)
WHERE toFloat(replace(p2.peso, ' kg', '')) >= 2 * toFloat(replace(p1.peso, ' kg', ''))
WITH p1, p2, length(path) AS num_evolucoes
WHERE num_evolucoes >= 1
WITH p1, collect({evolucao: p2, peso: p2.peso, num: num_evolucoes}) AS evolucoes
RETURN p1.nome AS Pokemon_Original,
       p1.peso AS Peso_Original,
       head(evolucoes).evolucao.nome AS Evolucao,
       head(evolucoes).peso AS Peso_Evolucao,
       head(evolucoes).num AS Numero_Evolucoes
ORDER BY p1.nome;
""")

print("Todos os Pokémons do Tipo Fire:\n")

consultar("""
MATCH (p:Pokemon)-[:TEM_TIPO]->(t:Tipo)
WHERE t.nome = 'Fire'
RETURN p.nome AS Nome_Pokemon, t.nome AS Tipo
ORDER BY p.nome
""")

print("Todos os Pokémons que tem no mínimo 2 evoluções:\n")

consultar("""
MATCH (p:Pokemon)-[:EVOLUI_PARA*1..]->(e:Pokemon)
WITH p, COUNT(DISTINCT e) AS num_evolucoes
WHERE num_evolucoes >= 2  // Filtra para Pokémons com pelo menos 2 evoluções
RETURN p.nome AS Nome_Pokemon,
       p.peso AS Peso_Pokemon,
       p.altura AS Altura_Pokemon,
       num_evolucoes AS Numero_Evolucoes
ORDER BY p.nome
""")

# Importa as bibliotecas necessárias do Apache Beam para processamento de dados em pipeline.
import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

# Configura as opções do pipeline.
pipeline_options = PipelineOptions(argv=None)

# Inicializa o pipeline.
pipeline = beam.Pipeline(options=pipeline_options)

# Definição das colunas do conjunto de dados de dengue.
colunas_dengue = [
    'id', 
    'data_iniSE', 
    'casos', 
    'ibge_code', 
    'CC', 
    'uf', 
    'cep', 
    'latitude', 
    'longitude'
]

# Função para transformar uma lista em um dicionário usando as colunas definidas.
def lista_para_dicionario(elemento, colunas):
    """
    Recebe uma lista e um conjunto de colunas
    Retorna um dicionário com as colunas como chaves
    """
    return dict(zip(colunas, elemento))

# Método com parâmetro para receber texto e um delimitador, retorna uma lista de elementos pelo delimitador.
def texto_para_lista(elemento, delimitador='|'):
    """
    Recebe uma string e um delimitador
    Retorna uma lista dos elementos divididos pelo delimitador
    """
    return elemento.split(delimitador)

def trata_datas(elemento):
    """
    Recebe um dicionário e cria um campo com ANO-MES
    Retorna o mesmo dicionário com o novo campo
    """
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-')[:2])
    return elemento

def chave_uf(elemento):
    """
    Recebe um dicionário
    Retorna uma tupla com o estado (uf) e o elemento (uf, dicionario)
    """
    chave = elemento['uf']
    return (chave, elemento)

def casos_dengue(elemento):
    """
    Recebe uma tupla ('RS', [{}, {}])
    Retorna uma tupla ('RS-2014-12', 8.0)
    """
    # Desempacota a tupla de entrada ('RS', [{}, {}]).
    uf, registros = elemento
    
    # Itera sobre os registros de casos de dengue.
    for registro in registros:
        # Verifica se o campo 'casos' contém dígitos.
        if bool(re.search(r'\d', registro['casos'])):
            # Se o campo 'casos' contém dígitos, cria uma tupla com a chave composta (uf-ano_mes) e o valor dos casos como um float.
            yield (f"{uf}-{registro['ano_mes']}", float(registro['casos']))
        else:
            # Se o campo 'casos' não contém dígitos, cria uma tupla com a chave composta (uf-ano_mes) e o valor dos casos como 0.0.
            yield (f"{uf}-{registro['ano_mes']}", 0.0)

# Leitura dos dados do arquivo 'casos_dengue.txt' e ignorando a primeira linha (cabeçalho).
dengue = (
    pipeline
    | "Leitura do Dataset dos casos de Dengue" >> ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_para_lista)
    | "De lista para dicionário" >> beam.Map(lista_para_dicionario, colunas_dengue)
    | "Criar campo ano_mes" >> beam.Map(trata_datas)
    | "Criar chave pelo estado" >> beam.Map(chave_uf)
    | "Agrupar pelo estado" >> beam.GroupByKey()
    | "Descompactar casos de dengue" >> beam.FlatMap(casos_dengue)
    | "Somar dos casos pela chave" >> beam.CombinePerKey(sum)
    | "Mostrar resultados" >> beam.Map(print)
)

chuvas = (
    pipeline
    | "Leitura do Dataset de chuvas" >> ReadFromText('chuvas.csv', skip_header_lines=1)
)

# Execução do pipeline.
pipeline.run()

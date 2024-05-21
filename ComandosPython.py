# Importa as bibliotecas necessárias do Apache Beam para processamento de dados em pipeline.
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
    return dict(zip(colunas, elemento))

# Método com parâmetro para receber texto e um delimitador, retorna uma lista de elementos pelo delimitador.
def texto_para_lista(elemento, delimitador='|'):
     """
    Recebe 2 lista
    Retorna 1 dicionário
    """
    return elemento.split(delimitador)

# Leitura dos dados do arquivo 'casos_dengue.txt' e ignorando a primeira linha (cabeçalho).
dengue = (
    pipeline
    | "Leitura do Dataset dos casos de Dengue" >> ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_para_lista)
    | "De lista para dicionário" >> beam.Map(lista_para_dicionario, colunas_dengue)
    | "Mostrar resultados" >> beam.Map(print)
)

# Execução do pipeline.
pipeline.run()

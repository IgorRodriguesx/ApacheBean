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

def trata_datas(elemento):
    """
    Recebe um dicionário e cria um campo com ANO-MES
    Retorna o mesmo dicionário com o novo campo
    """
    #"2016-08-01" >> ['2016','08'] >> "2016-08"
    elemento['ano_mes'] = '-'.join(elemento['data_iniSE'].split('-'))[:2]
    return elemento

def chave_uf(elemento):
    """
    Receber um dicionário
    Retornar uma tupla com o estado (uf) e o elemento (uf, dicionario)
    """
    chave = elemento['uf']
    return (chave,elemento)

def casos_dengue(elemento):
    """
    Recebe uma tupla ('RS', [{}, {}])
    Retornar uma tupla ('RS-2014-12', 8.0)
    """
    # uf recebeu o estado (RS) e registros recebeu a array [{},{}] 
    uf, registros = elemento
    # Percorrer a array (registros) usando o For e dando o nome de Registro
    for registro in registros:
        # Se fosse utilizado 'return' ele pararia no primeiro uf que achasse e dario o retorno, o yield faz percorrer todas as ufs
        # f"" > para formatar o retorno, {} - {} para indicar de onde será tirado o retorno (Váriavel registro, no campo 'ano_mes')
        yield (f"{uf}-{registro['ano_mes']}", registro['casos'])


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
    | "Mostrar resultados" >> beam.Map(print)
)

# Execução do pipeline.
pipeline.run()

# Importa as bibliotecas necessárias do Apache Beam para processamento de dados em pipeline.
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

# Configura as opções do pipeline.
pipeline_options = PipelineOptions(argv=None)

# Inicializa o pipeline.
pipeline = beam.Pipeline(options=pipeline_options)

# Método com parâmetro para receber texto e um delimitador, retorna uma lista de elementos pelo delimitador
def texto_para_lista(elemento, delimitador = '|') :
    
    return elemento.split(delimitador)

# Lê os dados do arquivo 'casos_dengue.txt' e ignora a primeira linha (cabeçalho).

dengue = (
    pipeline
    | "Leitura do Dataset dos casos de Dengue" >> ReadFromText('casos_dengue.txt', skip_header_lines=1)
    | "De texto para lista" >> beam.Map(texto_para_lista)
    | "Mostrar resultados" >> beam.Map(print)
)

pipeline.run()

import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv=none)
pipeline = beam.Pìpeline(options = pipeline_options)

dengue = ( pipeline | "Leitura do Dataset dos casos de Dengue" >> ReadFromText())

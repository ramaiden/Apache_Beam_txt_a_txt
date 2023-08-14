import apache_beam as beam
import argparse
import re

from apache_beam import PCollection
from apache_beam.options.pipeline_options import PipelineOptions

def main():
    # Estas tres lineas especifican que al momento de llamar la aplicación se deba especificar
    # las variables entrada y salida. En caso de que se corra en GCP estas variables cambian
    parser = argparse.ArgumentParser(description='Primer pipeline')
    parser.add_argument('--entrada', help='Fichero de entrada')
    parser.add_argument('--salida', help='fichero de salida')

    our_args ,beam_args =parser.parse_known_args()
    run_pipeline(our_args, beam_args) 

def run_pipeline(custom_args, beam_args):
    #beam_arg es el runner
    entrada=custom_args.entrada
    salida=custom_args.salida

    opts = PipelineOptions(beam_args)

    with beam.Pipeline(options=opts) as pipeline:
        counts = (
            pipeline
            | beam.io.ReadFromText('el_quijote.txt')
            | beam.FlatMap(lambda line: re.findall(r'\w+', line.lower()))
            #Se eliminan todas las mayusculas y signos
            | beam.Map(lambda word: (word, 1))
            # cada palabra se cuenta como uno
            | beam.CombinePerKey(sum)
            # Se suman todas por palabra
            | beam.transforms.combiners.ToList()
            # Se transforma a una lista para que permita ordenarla posteriormente
            | beam.Map(lambda word_count: sorted(word_count, key=lambda x: x[1], reverse=True))
            # Se ordena la lista
            | beam.Map(lambda word_count: ''.join([f'{word}: {count}\n' for word, count in word_count]))
            # Se aplica un formato de escritura
            | beam.io.WriteToText('word_counts.csv', file_name_suffix='.csv' )
        )


if __name__== '__main__':
    main()

import apache_beam as beam
from apache_beam.io.gcp.internal.clients import bigquery

table_spec = bigquery.TableReference(
    projectId='bee-bit-tech-2020',
    datasetId='apache_beam_interns',
    tableId='interns')

table_schema = {
    'fields': [{
        'name': 'nome', 'type': 'STRING', 'mode': 'REQUIRED'
    },
    {
        'name': 'idade', 'type': 'INTEGER', 'mode': 'REQUIRED'
    },
    {
       'name': 'estado', 'type': 'STRING', 'mode': 'REQUIRED'
    },
    {
        'name': 'linguagem', 'type': 'STRING', 'mode': 'NULLABLE'
    }]
}

with beam.Pipeline() as p:
    quotes = p | beam.Create([
        {
            'nome': 'João Victor', 'idade': 21, 'estado': 'SP', 'linguagem': 'Java'
        },
        {
            'nome': 'Rebeca Elizabeth', 'idade': 19, 'estado': 'SP', 'linguagem': 'C'
        },
        {
            'nome': 'Vinícius Gomes', 'idade': 20, 'estado': 'SP', 'linguagem': 'Java'
        },
        {
            'nome': 'Arthur Sanches', 'idade': 23, 'estado': 'SP', 'linguagem': 'Python'
        },
        {
            'nome': 'Tayná Martins', 'idade': 23, 'estado': 'RN', 'linguagem': 'PHP'
        },
        {
            'nome': 'Paola Frizon', 'idade': 19, 'estado': 'SP', 'linguagem': 'Java'
        },
        {
            'nome': 'Ana Paula', 'idade': 24, 'estado': 'PR', 'linguagem': 'Java'
        }
    ])

    quotes | beam.io.WriteToBigQuery(
        table_spec,
        schema=table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
    )
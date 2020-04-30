from google.cloud import bigquery

client = bigquery.Client()
dataset_ref = client.dataset('apache_beam')

table_ref = dataset_ref.table('pubsub_messages')
schema = [
    bigquery.SchemaField('username', 'STRING'),
    bigquery.SchemaField('message', 'STRING'),
    bigquery.SchemaField('date', 'TIMESTAMP'),
]

table = bigquery.Table(table_ref, schema=schema)
table.time_partitioning = bigquery\
                            .TimePartitioning(type_=bigquery.TimePartitioningType.DAY,
                                                    field='date',
                                                    expiration_ms=7776000000,
                                                    require_partition_filter=True)

table = client.create_table(table)

print('Table {} created, partitioned on column {}'.format(table.table_id, table.time_partitioning.field))






# | 'Split' >> (beam.FlatMap(find_words).with_output_types(unicode))
        #| 'PairWithOne' >> beam.Map(lambda x: (x, 1))
        #| beam.WindowInto(window.FixedWindows(2 * 60, 0))
        #| 'Group' >> beam.GroupByKey()
        #| 'Add username' >> beam.Map(add_user)
        #| 'Format' >> beam.ParDo(FormatDoFn()))


#| 'decode' >> beam.Map(lambda x: x.decode('utf-8'))
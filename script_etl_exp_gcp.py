from google.cloud import storage
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *



uri =  'gs://fernando-barbosa'
bucket = 'fernando-barbosa'
pasta = 'projeto_01'

storage_client = storage.Client()

bucket = storage_client.bucket(bucket)


list(bucket.list_blobs(prefix = pasta))

spark = SparkSession.builder.appName('ETL_Silver_Estudos_Fernando_GCP').getOrCreate()

df = spark.read.parquet('gs://fernando-barbosa/projeto_01/01_bronze/dados_bronze.parquet')


df = df \
        .withColumnRenamed("coAno","ano") \
        .withColumnRenamed("coMes","mes") \
        .withColumnRenamed("coNcm","cod_ncm") \
        .withColumnRenamed("noNcmpt","desc_item") \
        .withColumnRenamed("noUf","uf_origem") \
        .withColumnRenamed("noPaispt","pais") \
        .withColumnRenamed("noBlocopt","bloco_economico") \
        .withColumnRenamed("noVia","via") \
        .withColumnRenamed("noUrf","urf") \
        .withColumnRenamed("vlFob","valor_fob_us")

df = df\
        .withColumn('ano', df.ano.cast('int'))\
        .withColumn('mes', df.mes.cast('int'))\
        .withColumn('valor_fob_us', df.valor_fob_us.cast('float'))\
        .withColumn('kgLiquido', df.kgLiquido.cast('float'))

# Salvando dados - Silver
caminho = 'gs://fernando-barbosa/projeto_01/02_silver/dados_silver'

df.write.mode('overwrite').parquet(caminho)


df.printSchema()

def dim (df, dim):
    df.createOrReplaceTempView('data')
    x = spark.sql(f"SELECT DISTINCT {dim} AS test FROM data")
    x.createOrReplaceTempView('temp_view')
    dim_df = spark.sql(f"""
        SELECT 
            ROW_NUMBER() OVER (ORDER BY test) AS id_{dim},
            test AS {dim}
        FROM temp_view
    """)

    return  dim_df


dim_urf = dim(df, 'urf')
dim_uf = dim(df, 'uf_origem')
dim_bloco_economico = dim(df, 'bloco_economico')
dim_via = dim(df, 'via')
dim_pais = dim(df, 'pais')
dim_desc_item = dim(df, 'desc_item')


df_fato = df\
            .join(dim_urf, df.urf == dim_urf.urf, 'INNER')\
            .join(dim_uf, df.uf_origem == dim_uf.uf_origem, 'INNER')\
            .join(dim_bloco_economico, df.bloco_economico == dim_bloco_economico.bloco_economico, 'INNER')\
            .join(dim_via, df.via == dim_via.via, 'INNER')\
            .join(dim_pais, df.pais == dim_pais.pais, 'INNER')\
            .join(dim_desc_item, df.desc_item == dim_desc_item.desc_item, 'INNER')\
            .select(df.mes, df.ano, df.valor_fob_us, df.kgLiquido, df.cod_ncm, dim_desc_item.id_desc_item, 
                    dim_uf.id_uf_origem, dim_bloco_economico.id_bloco_economico, dim_via.id_via, dim_pais.id_pais, dim_urf.id_urf)


df_fato.printSchema()

#Enviando as tabelas Dimensões e Fato para o Google Cloud Storage
# Tabela Fato
caminho = 'gs://fernando-barbosa/projeto_01/03_gold/fato'
df_fato.write.mode('overwrite').parquet(caminho)

# Tabela Dimensôes
caminho = 'gs://fernando-barbosa/projeto_01/03_gold/dim_urf'
dim_urf.write.mode('overwrite').parquet(caminho)

caminho = 'gs://fernando-barbosa/projeto_01/03_gold/dim_uf'
dim_uf.write.mode('overwrite').parquet(caminho)

caminho = 'gs://fernando-barbosa/projeto_01/03_gold/dim_bloco_economico'
dim_bloco_economico.write.mode('overwrite').parquet(caminho)

caminho = 'gs://fernando-barbosa/projeto_01/03_gold/dim_via'
dim_via.write.mode('overwrite').parquet(caminho)

caminho = 'gs://fernando-barbosa/projeto_01/03_gold/dim_pais'
dim_pais.write.mode('overwrite').parquet(caminho)

caminho = 'gs://fernando-barbosa/projeto_01/03_gold/dim_desc_item'
dim_desc_item.write.mode('overwrite').parquet(caminho)
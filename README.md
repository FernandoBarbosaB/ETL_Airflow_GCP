# ETL de Dados sobre Exportações Airflow - Google Cloud Platform

Este documento demonstra o desenvolvimento técnico do projeto de ETL de dados sobre exportações de Carnes Bovinas durante o primeiro quadrimestre de 2023 utilizando os recursos de cloud da Google (GCP) e visualização em um dashboard.



Utilizando ferramentas como Airflow, Google Cloud Storage, Dataproc, BigQuery e PowerBi.

## 🏭 Arquitetura

![arquitetura-Página-2](https://github.com/FernandoBarbosaB/ETL_Airflow_GCP/assets/116772002/5ecff81b-58da-4338-aa3c-53782612a65e)


## ⚙️ Extração dos Dados

Processo de extração dos dados da API do site Comex Stat através do Apache Airflow.
http://comexstat.mdic.gov.br/pt/home


 

![dag01](https://github.com/FernandoBarbosaB/ETL_Airflow_GCP/assets/116772002/1b8d431c-8a43-4e9a-87c4-acaec8c98191)



### 🔩 Layout dos Dados




|   **variável**     | **tipo**  |               **descrição**                |
|:------------------:|:---------:|:------------------------------------------:|
|       mes          |   INTEGER | mês de referência da venda                 |
|       ano          |   INTEGER |  ano de referência da venda                |
|   valor_fob_us     |   FLOAT   |     valor em U$                            |
|    kgLiquido       |   FLOAT   |      Kilograma liquido                     |
|     cod_ncm        |   STRING  |   código Nomenclatura Comum do Mercosul    |
|     desc_item      |   STRING  |           descrição do item                |
|      uf_origem     |   STRING  |       estado de origem do produto          |
|    bloco_economico |   STRING  |         bloco econômico                    |
|     via            |   STRING  |    referente a via de transporte           |
|     pais           |   STRING  |    referente ao pais de destino            |
|     urf            |   STRING  |     referente ao local de saida do produto |
 
 




## 📦 Desenvolvimento



Para o processo de extração dos dados, utilizamos a API do site Comex Stat com o objetivo de obter informações sobre exportações de Carnes Bovinas durante o primeiro quadrimestre de 2023. Utilizamos o filtro da API para selecionar os dados de acordo com os códigos NCM (Nomenclatura Comum do Mercosul) específicos para os diferentes tipos de Carnes Bovinas.

A API do Comex Stat pode ser acessada através do seguinte link: http://api.comexstat.mdic.gov.br/general?filter{<>}

Aqui estão as descrições dos itens selecionados:

- Carcaças e meias carcaças de bovino, frescas ou refrigeradas
- Carcaças e meias-carcaças de bovino, congeladas
- Carnes desossadas de bovino, congeladas
- Carnes desossadas de bovino, frescas ou refrigeradas
- Fígados de bovino, congelados
- Línguas de bovino, congeladas
- Miudezas comestíveis de bovino, frescas ou refrigeradas
- Outras miudezas comestíveis de bovino, congeladas
- Outras peças não desossadas de bovino, congeladas
- Outras peças não desossadas de bovino, frescas ou refrigeradas
- Quartos dianteiros não desossadados de bovino, frescos/refrigerados
- Quartos dianteiros não desossados de bovino, congelados
- Quartos traseiros não desossados de bovino, congelados
- Quartos traseiros não desossados de bovino, frescos/refrigados
- Rabos de bovino, congelados


Para realizar a coleta dos dados brutos, utilizamos o Apache Airflow como orquestrador. Essa tarefa é executada pela task "Coleta de Dados" e os dados coletados são enviados para a camada bronze do Google Cloud Storage por meio da task "Envio GCS".

![bronze](https://github.com/FernandoBarbosaB/ETL_Airflow_GCP/assets/116772002/e43b8959-2fe7-4be1-87d7-decb5ca257ff)

![printschema1](https://github.com/FernandoBarbosaB/ETL_Airflow_GCP/assets/116772002/43c32f2f-2788-43fa-93d3-698103bf8c63)



A task de processamento dos dados executa um Job utilizando PySpark e SQL, utilizando o recurso Google Dataproc. Durante esse processamento inicial, os dados são tipados e as colunas são renomeadas. Após essa etapa de tratamento, os dados são salvos na camada Silver do Cloud Storage.


![silver](https://github.com/FernandoBarbosaB/ETL_Airflow_GCP/assets/116772002/20cf19db-c0cd-4f0c-bb39-48681a181ac6)


![printschema2](https://github.com/FernandoBarbosaB/ETL_Airflow_GCP/assets/116772002/46fbcb4f-3942-474c-9347-d7708e6e1c39)

Na etapa seguinte, é realizada a estruturação da modelagem dos dados, criando tabelas de dimensões e a tabela fato, que são então enviadas para a camada Gold do Cloud Storage.

![gold](https://github.com/FernandoBarbosaB/ETL_Airflow_GCP/assets/116772002/2819966f-3dcf-4cd6-af55-129cfa6a7fe2)


![printschema3](https://github.com/FernandoBarbosaB/ETL_Airflow_GCP/assets/116772002/3a0eef7d-b075-4212-a9f1-6e19f447d4d2)

![starschemapbi](https://github.com/FernandoBarbosaB/ETL_Airflow_GCP/assets/116772002/c56b90d1-24ec-41c7-af97-b4b217ab147c)



Foi criado um conjunto de dados no BigQuery para este projeto, onde armazenamos todas as informações relevantes. A imagem abaixo mostra a estrutura do conjunto de dados e as tabelas utilizadas:

![bq1](https://github.com/FernandoBarbosaB/ETL_Airflow_GCP/assets/116772002/42ee1304-ff16-46c5-85c4-f468a5541d95)

Por meio do Dashboard final desenvolvido no Power BI, é possível visualizar diversas métricas importantes. Algumas das métricas destacadas são:

- Somatório dos valores de exportação em dólar.
- Somatório do peso em quilogramas dos itens exportados.
- Top 5 estados que mais realizaram exportações.
- Top 5 países que mais consumiram os produtos exportados.

(imagem dashboard)

O Dashboard no Power BI oferece uma visão clara e intuitiva dessas métricas, permitindo uma análise mais aprofundada dos dados e facilitando a identificação de tendências e insights relevantes para o negócio.

## 🛠️ Construído com


* [Apache Airflow](https://airflow.apache.org/) - Ferramenta de orquestração de fluxo de trabalho para pipelines de engenharia de dados.
* [Google Cloud Storage](https://cloud.google.com/storage?hl=pt-br) - Seriço de Armazenamento de arquivos online da Google
* [Dataproc](https://cloud.google.com/dataproc?hl=pt-br) - Serviço gerenciado de processamento distribuido de Big Data com Spark
* [BigQuery](https://cloud.google.com/bigquery?hl=pt-br) - Data Warehouse em nuvem da Google
* [Power Bi](https://powerbi.microsoft.com/pt-br/) - Ferramenta de Visualização de Dados

* [Comex Stat](http://comexstat.mdic.gov.br/pt/home) - Portal para acesso gratuito às estatísticas de comércio exterior do Brasil


## 🏃 Autor


* **Fernando Barbosa** - *Engenheiro de Dados* - [github](https://github.com/FernandoBarbosaB)


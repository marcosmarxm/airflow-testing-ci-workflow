# Tutorial explicando como desenvolver uma DAG usando TDD

## Introdução

Nesse tutorial iremos construir a nossa DAG desde o início.
Se você quiser acompanhar aconselho a clonar o repositório e fazer o checkout para a branch `tutorial`.

```bash
git clone git@github.com:marcosmarxm/airflow-testing-ci-workflow.git
git checkout tutorial
```

Para tirar mais proveito você deva ter um conhecimento básico sobre **Airflow**, **python** e **pytest**.
Caso você não sabe, eu penso... que como vamos construindo aos poucos talvez você possa ir pesquisando e aprendendo os conceitos na hora.

Relembrando do pipeline que iremos desenvolver:

![Our DAG](../images/our_dag.png)

Explicando cada task:

- **load_full_products**: deleta os dados antigos e carrega a tabela `products` completamente todo dia.
- **load_incremental_purchases**: devido ao tamanho dessa tabela será realizado uma carga incremental utilizando o parâmetro de data `execution_date`.
- **join_purchase_products_as_product_sales_daily**: essa task intermediária prepara os dados brutos (products e purchases) carregados do dia do banco de dados `oltp` para serem armazenadas na tabela de resultados `product_sales` que será usada pelo time de analytics.
- **delete_products_sales_exec_date**: essa task tem a função de limpar os dados da tabela de resultado `product_sales` no início do pipeline, dessa forma garante que não terá dados duplicados (idempotência).
- **union_staging_to_products_sales**: carrega os dados do staging `product_sales_daily` para a tabela com dados históricos `product_sales`.
- **rebuild_agg_sales_category**: o resultado da tabela acima já ilustra um formato padrão de consumo para data warehouse, essa task ilustra a criação de um "data mart" simplificado.

## Vamos começar!

Primeiro vamos colocar nosso ambiente de desenvolvimento em pé.
Caso você tenha dúvidas sobre o ambiente recomendo ler novamente o artigo [How to develop data pipeline in Airflow through TDD (test-driven development)](https://blog.magrathealabs.com/how-to-develop-data-pipeline-in-airflow-through-tdd-test-driven-development-c3333439f358). Você pode ver o código no arquivo `Makefile`.

```bash
make setup
```

Isso irá demorar alguns minutos.
A imagem docker do Airflow 2.0 com o LocalExecutor está demorando para fazer a configuração inicial.
Após a configuração inicial enviamos alguns comandos para o Airflow: criação de usuário, criação das conexões e criação das variáveis.

Já temos um diagrama do pipeline esboçado.
Iremos pensar em como construir ele agora.
Também já vimos o formato dos dados e uma pequena amostra deles.
Com isso temos o insumo para realizarmos o início do desenvolvimento do projeto.

## TASK: Full load product data

A primeira tarefa que iremos desenvolver é a `full_load_product_data`.
Ela tem o objetivo de pegar os dados da tabela `products` do banco de dados `oltp-db` e transferir para o `olap-db`.
Primeiro vamos criar nossos dados fake para nos guiar.
Crie um arquivo no diretório `/data` chamado `products.csv`.
Você pode pegar os dados do [arquivo fornecido como exemplo no branch master](https://raw.githubusercontent.com/marcosmarxm/airflow-testing-ci-workflow/master/data/products.csv).
Exemplo abaixo:

|product_id|product_name         |product_category|
|----------|---------------------|----------------|
|220       |Brand A T-Shirt      |T-Shirt         |
|222       |Dri-FIT T-Shirt      |T-Shirt         |
|225       |Brand N T-shirt      |T-Shirt         |
|227       |Casual Shoes         |Shoes           |
|228       |Generic Running Shoes|Shoes           |

Após iremos começar o desenvolvimento da DAG utilizando a metodologia TDD.
Precisamos criar um teste, executar ele, e teremos uma falha.
Em seguida vamos programar para fazer a parte que falha no teste funcionar.
Entrando num looping teste/código de correção dos erros até finalizar o pipeline.
As vantagens são:

- rápido feedback do problema, teremos apenas um erro para resolver;
- construção gradativa do nosso código assegurando que ele funciona.

Crie um arquivo no diretório `/tests` chamado `test_sales_pipeline.py`.

```python
# test_sales_pipeline.py
class TestSalesPipeline:

    def test_validate_sales_pipeline(self):
        assert len(olap_product_size) == 5
```

**Refletindo**: O objetivo dessa tarefa é comparar os dados que estarão no banco `olap-db` na tabela `products` com os dados de amostra `/data/products.csv`.

`olap_product_size`: é a variável que estou planejando que receba os valores que devem ser transferidos, é provável que ela seja uma lista com valores ou um dataframe.
Vamos começar com o mais básico possível:

* Comparar nosso resultado `olap_product_size` e ver se ele tem todos os itens que esperamos que ele tenha. Como podemos ver nos dados de amostra `/data/products.csv` temos 5 entradas, por esse motivo queremos comparar o tamanho de `olap_product_size` com 5.

Podemos rodar pela primeira vez o nosso teste através do comando:

```bash
make testing
```

Teremos como resultado que a variável `olap_product_size` não existe.
No projeto essa variável deve buscar os dados do banco de dados `olap-db` na tabela `products`.
Então, precisamos criar uma conexão com o banco `olap-db` e buscar esses valores.

Já  que estamos usando o Airflow vamos utilizar os **Hooks** que possuem diversos métodos de interação com os banco de dados.
Como já configuramos o container (olap-db) e a conexão do Airflow com ele no setup será bem simples completar essa etapa.
Iremos utilizar o **PostgresHook**, se quiser saber mais sobre Hooks pode acessar a [documentação do Airflow](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html?highlight=hook#hooks).

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

class TestSalesPipeline:

    def test_validate_sales_pipeline(self):
        olap_hook = PostgresHook('olap')
        olap_product_size = olap_hook.get_records(
            'select * from products'
        )
        assert len(olap_product_size) == 5
```

Importamos o **PostgresHook** e criamos o hook para o banco de dados `olap-db`.
Esse hook possui um método que consegue executar uma query SQL e retornar os valores dela.
Após editar o arquivo de teste conforme apresentado acima podemos rodar `make testing` novamente.
Receberemos o erro que a tabela `products` não existe no banco `olap-db`.

**Ponto de atenção** Aqui vem uma consideração importante sobre testes.
O nosso pipeline é responsável por transferir os dados e não criar essas tabelas.
Então faz parte do teste configurar esse setup das tabelas.

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

class TestSalesPipeline:

    def test_validate_sales_pipeline(self):
        olap_hook = PostgresHook('olap')
        olap_hook.run('''
        CREATE TABLE IF NOT EXISTS products (
            product_id       INTEGER,
            product_name     TEXT,
            product_category TEXT
        );
        ''')
        olap_product_size = olap_hook.get_records(
            'select * from products'
        )
        assert len(olap_product_size) == 5
```

O comando `.run(sql statement)` executa um query SQL no banco de dados. Ele é parecido com o `.get_records` que vimos antes, entretanto serve para quando não queremos os dados de retorno.
No exemplo ele irá criar a tabela `products` com as colunas necessárias conforme nossos dados de amostra `/data/products.csv`.

Rodamos novamente os testes e agora o erro que temos é que o existe uma diferença entre o `olap_product_size` e o valor que esperamos seja igual a 5.
Nesse estágio chegamos a necessidade de iniciar a nossa DAG pois já configuramos o inicialmente nosso teste.
Iremos criar um arquivo chamado `dag_sales_pipeline.py` dentro do diretório `/dags`.

```python
from airflow import DAG
from airflow.utils.dates import days_ago

with DAG(dag_id='products_sales_pipeline',
         default_args={'owner': 'airflow'},
         schedule_interval=None,
         start_date=days_ago(2)) as dag:
```

O código acima apenas instancia uma nova DAG básica.
Precisamos pensar agora em como resolver nosso problema.
Necessitamos de uma função que transfira os dados do banco `oltp-db` para o `olap-db`.
Já vimos que os **Hooks** no Airflow possuem métodos que podem nos auxiliar: executar um sql e pegar os dados, executar um sql sem retorno dos dados, entre outras interações com o banco de dados.

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


def transfer_oltp_olap(**kwargs):
    """Get records from OLTP and transfer to OLAP database"""
    dest_table = kwargs.get('dest_table')
    sql = kwargs.get('sql')

    oltp_hook = PostgresHook(postgres_conn_id='oltp')
    olap_hook = PostgresHook(postgres_conn_id='olap')

    data_extracted = oltp_hook.get_records(sql=sql)
    olap_hook.insert_rows(dest_table, data_extracted, commit_every=1000)


with DAG(dag_id='products_sales_pipeline',
         default_args={'owner': 'airflow'},
         schedule_interval=None,
         start_date=days_ago(2)) as dag:

    load_full_products_data = PythonOperator(
        task_id='load_full_products',
        python_callable=transfer_oltp_olap,
        op_kwargs={
            'dest_table': 'products',
            'sql': 'select * from products',
        })
```

Explicando o que foi realizado:

1. Criamos a **task** `load_full_products_data`, que é um PythonOperator. Um **Operator** é um conceito no Airflow  que consegue invocar comandos básicos/padronizados. Por exemplo o **PythonOperator** chama funções em `python` e o **PostgresOperator** consegue executar queries SQL porém não consegue transferir dados de um banco de dados para outro. Para mais informações recomendo ler a [documentação](https://airflow.apache.org/docs/apache-airflow/stable/concepts.html?highlight=hook#operators).
2. Criamos a função `transfer_oltp_olap`, que basicamente cria os dois hooks para executar a coleta dos dados no banco `oltp-db` para o `olap-db`. Por que não utilizamos um **PostgresOperator**? O motivo é que o operator só consegue executar a query no limite do banco que ele está associado, ele não transfere dados. Por isso utilizamos os hooks. Os _kwargs_ é uma convenção do Airflow para passar os argumentos em funções chamadas pelo **PythonOperator**.

Após concluir a DAG podemos acessar o Airflow em http://localhost:8080, usando as credenciais *admin/admin*, e verificar que nossa primeira DAG estará lá!

![Our initial DAG](../images/our_initial_dag.png)

Ao ativá-la e executá-la no UI do Airflow, será registrado o seguint erro nos logs:

![Our initial DAG](../images/airflow_first_exec_error.png)

Vamos avaliar o log do Airflow e identificamos que não foi encontrada a tabela `products` no banco `oltp-db`.
É a mesma situação que a anterior: precisamos criar essa tabela na nossa função de teste.
Então vamos lá alterar novamente o `test_sales_pipeline.py`.

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook

class TestSalesPipeline:

    def test_validate_sales_pipeline(self):
        oltp_hook = PostgresHook('oltp')
        oltp_hook.run('''
        CREATE TABLE IF NOT EXISTS products (
            product_id       INTEGER,
            product_name     TEXT,
            product_category TEXT
        );
        ''')

        olap_hook = PostgresHook('olap')
        olap_hook.run('''
        CREATE TABLE IF NOT EXISTS products (
            product_id       INTEGER,
            product_name     TEXT,
            product_category TEXT
        );
        ''')

        olap_product_size = olap_hook.get_records(
            'select * from products'
        )
        assert len(olap_product_size) == 5
```

Criamos o hook para acessar o `oltp-db` e criamos a tabela `products` nele.
Rodamos o teste e obtemos o mesmo erro que o tamanho é diferente de 5.
Porém, agora temos nossa DAG e a tabela `products` nos dois bancos.
Se executarmos a DAG no UI do Airflow ela irá ter sucesso na execução.
Agora precisamos fazer ela ser executada durante o nosso teste.

O Airflow disponibiliza diversos comandos através do seu **cli** (comandos pelo terminal). O comando `airflow dags backfill --start_date DAG_ID` permite disparar uma DAG em uma data especifica (vide [documentação](https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html#backfill)).
Esse comando é perfeito para o nosso caso.

Podemos executar esse comando no terminal... então iremos aproveitar do Python e executar ele através da biblioteca _subprocess_.

```python
import subprocess
from airflow.providers.postgres.hooks.postgres import PostgresHook

def execute_dag(dag_id, execution_date):
    """Execute a DAG in a specific date this process wait for DAG run or fail to continue"""
    subprocess.run(["airflow", "dags", "backfill", "-s", execution_date, dag_id])

class TestSalesPipeline:

    def test_validate_sales_pipeline(self):
        oltp_hook = PostgresHook('oltp')
        oltp_hook.run('''
        CREATE TABLE IF NOT EXISTS products (
            product_id       INTEGER,
            product_name     TEXT,
            product_category TEXT
        );
        ''')

        olap_hook = PostgresHook('olap')
        olap_hook.run('''
        CREATE TABLE IF NOT EXISTS products (
            product_id       INTEGER,
            product_name     TEXT,
            product_category TEXT
        );
        ''')
        date = '2020-01-01'
        execute_dag('products_sales_pipeline', date)

        olap_product_size = olap_hook.get_records(
            'select * from products'
        )
        assert len(olap_product_size) == 5
```

Criamos uma função para nos auxiliar a invocar a execução da DAG durante o teste.
Assim quando executarmos o `make testing`, a DAG será executada automaticamente com a data que passamos, no caso `2020-01-01`.

O teste irá retornar **FAILED**.

Nós já criamos as duas tabelas, entretanto o banco de dados `oltp-db` não possui nenhum registro.
Precisamos conseguir inserir os dados fake nele.
Já criamos o arquivo `/data/products.csv`, mas precisamos transportar seus dados para dentro do `oltp-db`.
A forma mais simples que me vem na mente é ler o arquivo *csv* usando a biblioteca _pandas_ e transferir os dados para o banco usando a API do _pandas_.

```python
import subprocess
import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook


def execute_dag(dag_id, execution_date):
    """Execute a DAG in a specific date this process wait for DAG run or fail to continue"""
    subprocess.run(["airflow", "dags", "backfill", "-s", execution_date, dag_id])


class TestSalesPipeline:

    def test_validate_sales_pipeline(self):
        oltp_hook = PostgresHook('oltp')
        oltp_hook.run('''
        CREATE TABLE IF NOT EXISTS products (
            product_id       INTEGER,
            product_name     TEXT,
            product_category TEXT
        );
        ''')

        oltp_conn = oltp_hook.get_sqlalchemy_engine()
        sample_data = pd.read_csv('./data/products.csv')
        sample_data.to_sql(
            name='products',        # nome da tabela SQL
            con=oltp_conn,          # conexão SQLalchemy
            if_exists='replace',    # garante que toda vez teremos os mesmos dados
            index=False             # não queremos salvar os indices do pandas no banco
        )

        olap_hook = PostgresHook('olap')
        olap_hook.run('''
        CREATE TABLE IF NOT EXISTS products (
            product_id       INTEGER,
            product_name     TEXT,
            product_category TEXT
        );
        ''')
        date = '2020-01-01'
        execute_dag('products_sales_pipeline', date)

        olap_product_size = olap_hook.get_records(
            'select * from products'
        )
        assert len(olap_product_size) == 5
```

Como havia comentado, os hooks do Airflow possuem diversos métodos que auxiliam na comunicação e operações com os bancos. Nesse caso facilmente criamos uma *engine SQLAlchemy* para o _pandas_ enviar os dados do *csv* para a tabela `products` no banco de dados `oltp-db`.

Agora, momento de tensão... executamos novamente `make testing`... E NOSSO **TESTE PASSOU!**

O 1 warning é devido ao hook do Airflow utilizar o formato antigo de conexão com o banco Postgresql.

```text
======================== 1 passed, 1 warning in 11.06s =========================
```

Para verificarmos realmente, podemos acessar o banco `olap-db` através do comando no terminal:

```bash
docker exec -ti olap-db psql -U root olap
```

e depois executando `select * from products;` teremos o seguinte resultado.

![Our first passed test output](../images/output_products.png)

Muito bem! Finalmente temos nossa DAG executando a primeira tarefa da forma que esperamos.
Precisamos agora desenvolver as próximas tarefas.
Como já temos o alicerce construído será mais rápido e descomplicado realizar as próximas tarefas.

**Ponto de atenção: criamos um teste bastante simplista**.

Seria melhor realizar uma comparação que garanta o resultado da DAG seja compatível com o dado que realmente esperamos.
Nessa tarefa, queremos que os dados da tabela `products` no banco de dados `olap-db` sejam iguais aos do arquivo `/data/products.csv`. Vamos fazer isso agora.

```python
import subprocess
import pandas as pd
from pandas._testing import assert_frame_equal
from airflow.providers.postgres.hooks.postgres import PostgresHook


def execute_dag(dag_id, execution_date):
    """Execute a DAG in a specific date this process wait for DAG run or fail to continue"""
    subprocess.run(["airflow", "dags", "backfill", "-s", execution_date, dag_id])


class TestSalesPipeline:

    def test_validate_sales_pipeline(self):
        oltp_hook = PostgresHook('oltp')
        oltp_hook.run('''
        CREATE TABLE IF NOT EXISTS products (
            product_id       INTEGER,
            product_name     TEXT,
            product_category TEXT
        );
        ''')
        oltp_conn = oltp_hook.get_sqlalchemy_engine()
        sample_data = pd.read_csv('./data/products.csv')
        sample_data.to_sql('products', con=oltp_conn, if_exists='replace', index=False)

        olap_hook = PostgresHook('olap')
        olap_hook.run('''
        CREATE TABLE IF NOT EXISTS products (
            product_id       INTEGER,
            product_name     TEXT,
            product_category TEXT
        );
        ''')
        date = '2020-01-01'
        execute_dag('products_sales_pipeline', date)

        # Renomeado variável para _size
        olap_product_size = olap_hook.get_records(
            'select * from products'
        )
        assert len(olap_product_size) == 5

        # Novo teste
        olap_product_data = olap_hook.get_pandas_df('select * from products')
        assert_frame_equal(olap_product_data, sample_data)
```

1. importamos `from pandas._testing import assert_frame_equal` para nos auxiliar a comparar um dataframe pandas.
2. criamos a variável `olap_product_data` usando novamente o hook porém agora retornando os dados do banco como um dataframe.
3. como já tínhamos carregado os dados do arquivo `/data/products.csv` para a variável `sample_data` facilitou executarmos a comparação.

Agora finalmente um teste que compara realmente se o que esperamos está sendo executado.

**Agora precisamos parar, pensar e respirar.**

Olhando a próxima tarefa (`load_incremental_purchases`) ela terá os praticamente os mesmos passos.
No código de teste existem várias partes que podem ser refatoradas, modularizando em funções para que sejam reaproveitadas para a próxima tarefa.
Vamos fazer isso. As atividades que serão realizadas:

* transferir os comandos sql para arquivos deixando o código mais organizado;
* vamos criar arquivos específicos para o resultado **esperado** que iremos comparar com o resultado das tarefas.
* a função (`create_table`) que cria uma tabela em determinado banco de dados, ela recebe o nome do arquivo sql (descrito no item acima) que também será o nome da tabela e o hook do banco de dados para executar a operação;
* a função (`insert_initial_data`) que insere os dados iniciais na tabela especificada;
* a função (`output_expected_as_df`) que pega os dados esperados para comparar com o resultado da DAG.

Primeiro vamos colocar os scripts de criação das tabelas em arquivos.
Crie um arquivo no path e chamado: `/sql/init/create_products.sql`

```sql
CREATE TABLE IF NOT EXISTS products (
    product_id       INTEGER,
    product_name     TEXT,
    product_category TEXT
);
```

Crie uma pasta `expected` dentro da `/data`.
Nesse caso vamos apenas duplicar o arquivo `products.csv` para dentro dela.

Após essas etapas voltamos a editar o nosso teste.

```python
import subprocess
import pandas as pd
from pandas._testing import assert_frame_equal
from airflow.providers.postgres.hooks.postgres import PostgresHook


def insert_initial_data(tablename, hook):
    """This script will populate database with initial data to run job"""
    conn_engine = hook.get_sqlalchemy_engine()
    sample_data = pd.read_csv(f'/opt/airflow/data/{tablename}.csv')
    sample_data.to_sql(name=tablename, con=conn_engine, if_exists='replace', index=False)


def create_table(tablename, hook):
    sql_stmt = open(f'/opt/airflow/sql/init/create_{tablename}.sql').read()
    hook.run(sql_stmt)


def output_expected_as_df(filename):
    return pd.read_csv(f'/opt/airflow/data/expected/{filename}.csv')


def execute_dag(dag_id, execution_date):
    """Execute a DAG in a specific date this process wait for DAG run or fail to continue"""
    subprocess.run(["airflow", "dags", "backfill", "-s", execution_date, dag_id])


class TestSalesPipeline:

    def test_validate_sales_pipeline(self):
        oltp_hook = PostgresHook('oltp')
        create_table('products', oltp_hook)
        insert_initial_data('products', oltp_hook)

        olap_hook = PostgresHook('olap')
        create_table('products', olap_hook)

        date = '2020-01-01'
        execute_dag('products_sales_pipeline', date)

        olap_product_size = olap_hook.get_records('select * from products')
        assert len(olap_product_size) == 5

        expected_product_data = output_expected_as_df('products')
        olap_product_data = olap_hook.get_pandas_df('select * from products')
        assert_frame_equal(olap_product_data, expected_product_data)

```

Nosso teste refatorado com funções que serão reaproveitadas nas próximas etapas.
Está bem mais legível separado em funções.
Tome um tempo e estude as mudanças que ocorreram.
Isso ajudará muito quem está começando entender e desbravar esse processo de refatoração. (Se tiver uma dúvida cruel pode me enviar uma mensagem)

## TASK: Load incremental purchases

Vamos começar a próxima tarefa!
A única diferença dela para anterior é que teremos uma condição na carga dos dados.
Devemos apenas carregar os dados do dia de execução, `execution_date`.
Primeiro vamos criar nosso arquivo com dados fake.
Crie o arquivo `purchases.csv` dentro do diretório `/data`.
Você pode pegar os dados do [arquivo fornecido como exemplo](https://raw.githubusercontent.com/marcosmarxm/airflow-testing-ci-workflow/master/data/purchases.csv).

|purchase_id|purchase_date|user_id|product_id|unit_price|quantity|total_revenue|
|--------------|-------------|-------|----------|----------|--------|-------------|
|1             |2020-01-01   |111    |222       |150.0     |2       |300.0        |
|2             |2020-01-01   |101    |225       |75        |1       |75           |
|3             |2020-01-01   |153    |228       |300       |1       |300          |
|4             |2020-01-10   |111    |227       |500       |1       |500          |
|5             |2020-01-10   |199    |222       |150       |3       |450          |
|6             |2020-01-10   |182    |220       |35        |4       |140          |
|7             |2020-01-10   |174    |222       |150       |1       |150          |
|8             |2020-01-15   |166    |227       |500       |1       |500          |
|9             |2020-01-15   |132    |225       |75        |1       |75           |
|10            |2020-01-15   |188    |220       |35        |10      |350          |

Abaixo temos nossa classe de teste (as outras funções e importações foram omitidas para diminuir o tamanho).
Começamos novamente uma nova etapa de testes.

```python
class TestSalesPipeline:

    def test_validate_sales_pipeline(self):
        oltp_hook = PostgresHook('oltp')
        olap_hook = PostgresHook('olap')

        create_table('products', oltp_hook)
        create_table('products', olap_hook)
        insert_initial_data('products', oltp_hook)

        date = '2020-01-01'
        execute_dag('products_sales_pipeline', date)

        # Test load_full_products task
        olap_products_size = olap_hook.get_records('select * from products')
        assert len(olap_products_size) == 5

        expected_products_data = output_expected_as_df('products')
        olap_products_data = olap_hook.get_pandas_df('select * from products')
        assert_frame_equal(olap_products_data, expected_products_data)

        # New code!
        # Test load_incremental_purchases
        olap_purchases_size = olap_hook.get_records('select * from purchases')
        assert len(olap_purchases_size) == 3
```

A coluna dos dados que correspondem ao tempo se chama `purchase_date`.
Então se analisarmos os dados de amostra temos apenas 3 entradas para data `2020-01-01`.
Essa data já estamos utilizando quando chamamos nossa DAG, variável `date = '2020-01-01'`.

Vou antecipar alguns passos que já fizemos com a DAG anterior. Vou criar a tabela `purchases` nos dois bancos de dados usando o arquivo `sql/init/create_purchases.sql`:

```sql
CREATE TABLE IF NOT EXISTS purchases (
    purchase_id      INTEGER,
    purchase_date    TEXT,
    user_id          INTEGER,
    product_id       INTEGER,
    unit_price       REAL,
    quantity         INTEGER,
    total_revenue    REAL
)
```

Depois, popular o banco de dados `oltp-db` com os dados fake que criamos. Foram incluídas as linhas abaixo:

```python
# test_sales_pipeline
class TestSalesPipeline:

    def test_validate_sales_pipeline(self):
        oltp_hook = PostgresHook('oltp')
        olap_hook = PostgresHook('olap')

        create_table('products', oltp_hook)
        create_table('products', olap_hook)
        insert_initial_data('products', oltp_hook)

        create_table('purchases', oltp_hook)
        create_table('purchases', olap_hook)
        insert_initial_data('purchases', oltp_hook)
```

Vamos adicionar a nova task à DAG.

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator


def transfer_oltp_olap(**kwargs):
    """Get records from OLTP and transfer to OLAP database"""
    dest_table = kwargs.get('dest_table')
    sql = kwargs.get('sql')
    params = kwargs.get('params')

    oltp_hook = PostgresHook(postgres_conn_id='oltp')
    olap_hook = PostgresHook(postgres_conn_id='olap')

    data_extracted = oltp_hook.get_records(sql=sql, parameters=params)
    olap_hook.insert_rows(dest_table, data_extracted, commit_every=1000)


with DAG(dag_id='products_sales_pipeline',
         default_args={'owner': 'airflow'},
         schedule_interval=None,
         start_date=days_ago(2)) as dag:

    execution_date = '{{ ds }}'

    load_full_products_data = PythonOperator(
        task_id='load_full_products',
        python_callable=transfer_oltp_olap,
        op_kwargs={
            'dest_table': 'products',
            'sql': 'select * from products',
        })

    load_incremental_purchases_data = PythonOperator(
        task_id='load_incremental_purchases',
        python_callable=transfer_oltp_olap,
        op_kwargs={
            'dest_table': 'purchases',
            'sql': 'select * from purchases where "purchase_date" = %s',
            'params': [execution_date]
        })
```

Foi criada uma nova task PythonOperator chamada `load_incremental_purchases_data`. Ela reutiliza a função `transfer_oltp_olap` criada anteriormente.
As únicas diferenças foram a cláusula `where purchase_data = %s` e a edição da função para receber o parâmetro extra na consulta.
A sintaxe `{{ ds }}` é uma convenção do Airflow para acessar variáveis de contexto.
Existem diversas variáveis que podem ser acessadas dentro do contexto da DAG.
É meio obscuro no início, para mais informações leia a documentação [Macros Reference](https://airflow.apache.org/docs/apache-airflow/stable/macros-ref.html).

Podemos rodar nossos testes agora.
Nossa segunda task está concluída.
Novamente podemos incrementar nosso teste para atender melhor o projeto.

Nesse caso, vamos criar o arquivo com os dados esperados.
Ao invés de copiar todo o arquivo `purchases.csv` como aconteceu com os dados de produtos, agora iremos apenas precisar de um subconjunto pertinente aos testes.
Crie um novo arquivo chamado `purchases_2020-01-01.csv` dentro da pasta `expected`.

|purchase_id|purchase_date|user_id|product_id|unit_price|quantity|total_revenue|
|--------------|-------------|-------|----------|----------|--------|-------------|
|1             |2020-01-01   |111    |222       |150.0     |2       |300.0        |
|2             |2020-01-01   |101    |225       |75        |1       |75           |
|3             |2020-01-01   |153    |228       |300       |1       |300          |

Ele terá apenas dados do dia 2020-01-01.
Agora vamos editar a nossa função de teste.

```python
# test_sales_pipeline.py

# old
olap_purchases_size = olap_hook.get_records('select * from purchases')
assert len(olap_purchases_size) == 3

# new
purchase_data = olap_hook.get_pandas_df('select * from purchases')
purchase_size = len(purchase_data)
purchase_expected = output_expected_as_df(f'purchases_{date}')
assert_frame_equal(purchase_data, purchase_expected)
assert purchase_size == 3
```

Assim concluímos a segunda tarefa.
Chegamos no estágio do projeto em que finalizamos as tarefas de extração e carga dos dados.
As próximas tarefas irão envolver apenas o banco `olap-db`.
Agora vamos utilizar outro recurso do Airflow para executar ações.

## TASK: join_purchases_products

Objetivo dessa task é realizar o join das duas tabelas criadas anteriormente.
Voltamos ao nosso arquivo de teste criando o novo teste para a tabela `join_purchases_products`.

```python
# test_sales_pipeline.py
# ...
purchase_data = olap_hook.get_pandas_df('select * from purchases')
purchase_size = len(purchase_data)
purchase_expected = output_expected_as_df(f'purchases_{date}')
assert_frame_equal(purchase_data, purchase_expected)
assert purchase_size == 3

# Test join_purchases_products
purchases_products_size = olap_hook.get_pandas_df('select * from join_purchases_products')
assert len(purchases_products_size) == 3
```

Explicando o motivo de esperar que o resultado seja 3.
Nesse join iremos pegar as transações carregadas e fazer um `left` join com a tabela de produtos. Por isso o tamanho máximo será 3.

Podemos editar a DAG após inserir o teste.

```python
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator

def transfer_oltp_olap(**kwargs):
    # não foi alterado nesse momento

with DAG(dag_id='products_sales_pipeline',
         default_args={'owner': 'airflow'},
         schedule_interval=None,
         template_searchpath='/opt/airflow/sql/sales/',
         start_date=days_ago(2)) as dag:

    execution_date = '{{ ds }}'

    load_full_products_data = PythonOperator(
        task_id='load_full_products',
        python_callable=transfer_oltp_olap,
        op_kwargs={
            'dest_table': 'products',
            'sql': 'select * from products',
        })

    load_incremental_purchases_data = PythonOperator(
        task_id='load_incremental_purchases',
        python_callable=transfer_oltp_olap,
        op_kwargs={
            'dest_table': 'purchases',
            'sql': 'select * from purchases where "purchase_date" = %s',
            'params': [execution_date]
        })

    join_purchases_with_products = PostgresOperator(
        task_id='join_purchases_products',
        postgres_conn_id='olap',
        sql='join_purchases_with_products.sql'
    )

    [load_full_products_data, load_incremental_purchases_data] >> join_purchases_with_products
```

1. `template_searchpath='/opt/airflow/sql/sales/',` que foi inserido na criação da DAG(...) as dag. Esse comando permite carregar scripts SQL de outra pasta.
2. **PostgresOperator** como agora iremos transformar os dados que estão no banco de dados `olap-db` podemos utilizar o Operator.
3. Por último foi realizado a conexão de dependência das tarefas.

Precisamos criar nosso arquivo SQL com a query.
Crie ele no diretório `/sql/sales/join_purchases_with_products.sql`.
Por que as pastas `init` e `sales`? Eu gosto de deixar os arquivos separados por esses segmentos lógicos onde eles são utilizados.

```sql
create table if not exists join_purchases_products as (
    select
        t.*,
        p.product_name,
        p.product_category
    from purchases t
    left join products p
        on p.product_id = t.product_id
)
```

Após criar o arquivo SQL podemos executar os testes e teremos nossa terceira tarefa concluída!
As próximas tarefas podem ser realizadas da mesma forma utilizando o PostgresOperator.
Vou deixá-las como desafio.
Caso tenha dificuldade você pode analisar o código que está no repositório para se guiar.

---
Muito obrigado e caso tenha alguma sugestão me envie uma mensagem pelo [LinkedIn](https://www.linkedin.com/in/marcos-marx-millnitz/?locale=en_US).

# Tutorial explaining how to develop a dag using TDD

Nesse tutorial iremos construir a DAG desde o inicio. Se você quiser acompanhar aconselho a clonar o repositorio e fazer o checkout para a branch `tutorial`. Ela está sem os arquivos que iremos construir.
Você precisa ter um conhecimento basico prévio sobre o Airflow, python e pytest.

Primeiro colocando o ambiente de pé, executando: 
```
make setup
```
irá demorar alguns minutos. A imagem atual do Airflow está com algum bug e está travando o banco de dados.

Antes de mais nada precisamos conversar sobre a tarefa. Já temos um desenho do pipeline realizado. Então precisamos pensar em como conseguir construir ele agora. Também já vimos o formato dos dados e uma pequena amostra.

A primeira tarefa que iremos executar é a `full_load_product_data`. Ela basicamente tem o objetivo de pegar os dados do banco oltp-db e transferir para o olap-db.
Então primeiramente vamos criar nossos dados fake, crie um arquivo no diretorio `/data` chamado `products.csv`. Você pode pegar os dados [aqui](https://github.com/marcosmarxm/airflow-testing-ci-workflow/blob/master/data/products.csv)

|product_id|product_name                 |product_category|
|----------|-----------------------------|----------------|
|220       |camiseta nike                |camiseta        |
|222       |tenis adidas                 |tenis           |
|225       |camiseta adidas              |camiseta        |
|227       |new tenis nike               |tenis           |
|228       |bermuda adidas               |bermuda         |

Após iremos começar nossos testes. Crie um arquivo no diretorio `/tests` chamado `test_sales_pipeline.py`
E vamos criar nosso primeiro teste! Seguindo a metodologia TDD precisamos criar um teste e rodar ele. Ele irá falhar. Então corrigimos o erro e adicionamos o proximo passo e vamos de forma iterativa até convergimos ao código esperado executando o que precisamos.

Nosso objetivo dessa tarefa é comparar os dados que estarão no banco OLAP-DB com os dados de amostra `/data/products.csv`.  
```python
import pandas as pd

class TestSalesPipeline:

    def test_validate_sales_pipeline(self):
        assert len(olap_product_data) == 5
```
Vamos começar com o mais basico possivel, comparar nosso output e ver se ele tem todos os itens que esperamos que ele tenha.


Se rodarmos o comando `make testing` teremos como resultado que a variavel `expected_product_data` não existe. Sabemos que esse resultado deve ser obtido do banco de dados `olap-db`. Então precisamos criar uma conexão com o banco e tentar buscar esses valores. Para facilitar já que estamos usando o Airflow, vamos utilizar os hooks que possuem diversos métodos que nos ajudam. 

```python
from airflow.providers.postgres.hooks.postgres import PostgresHook
 
class TestSalesPipeline:

    def test_validate_sales_pipeline(self):
        olap_hook = PostgresHook('olap')
        olap_product_data = olap_hook.get_records(
            'select * from products'
        )
        assert len(olap_product_data) == 5
```
Após rodar `make testing` novamente você irá receber o erro que a tabela `products`. Aqui vem uma consideração importante sobre testes. O nosso pipeline é responsavel por transferir apenas os dados e não criar as tabelas. Então faz parte do teste conseguir configurar esse setup.

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
        olap_product_data = olap_hook.get_records(
            'select * from products'
        )
        assert len(olap_product_data) == 5
```
Rodamos novamente os testes e agora o erro que temos é que o existe uma diferença entre o olap_product_data e o experado que é 5. Nesse estágio chegamos a necessidade da nossa DAG. Iremos criar um arquivo chamado `dag_sales_pipeline.py` dentro do diretorio `/dags`.
```python
from airflow import DAG
from airflow.utils.dates import days_ago

with DAG(dag_id='products_sales_pipeline',
         default_args={'owner': 'airflow'},
         schedule_interval=None,
         start_date=days_ago(2)) as dag:
```
O código acima apenas instância uma nova DAG. Precisamos pensar agora em como resolver nosso problema. Precisamos de uma função que transfira os dados do banco OLTP para o OLAP. Para isso iremos criar uma função em Python para executar essa transferencia.
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
Mesmo surgindo bastante código ele é bastante direto. 
1. A função `transfer_oltp_olap` precisa receber os parametros extras enviados pelo PythonOperator op_kwargs.
2. Criamos os hooks para executar operações no banco.
3. Extraimos os dados do banco `oltp-db` armazenamos na variavel `data_extracted`
4. Transferimos para o banco `olap-db` os dados.

Após concluir a DAG podemos acessar o Airflow UI e verificar que temos nossa DAG lá
![Our initial DAG](../images/our_initial_dag.png)

Podemos ativa-la e executar. E teremos nosso erro
![Our initial DAG](../images/airflow_first_exec_error.png)

Vamos avaliar o log do Airflow e identificamos que não foi encontrado a tabela `products` no banco `oltp-db`. Esse é o mesmo caso, precisamos criar essa tabela no teste. Então vamos lá alterar no `test_sales_pipeline.py`.
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

        olap_product_data = olap_hook.get_records(
            'select * from products'
        )
        assert len(olap_product_data) == 5
```
Rodamos o teste e obtemos o mesmo erro que o tamanho é diferente de 5. Agora que temos nossa DAG (mesmo ela dando erro) precisamos conseguir chamar elas no nosso teste. O Airflow permite executar diversos comandos pelo cli. Um desses comandos permite disparar a DAG em uma data especifica. Para nós esse comando é o ideal. [Link para doc](https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html#backfill). Esse comando precisa ser executado no terminal... então iremos aproveitar do python e fazer ele executar esse serviço para nós.
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

        olap_product_data = olap_hook.get_records(
            'select * from products'
        )
        assert len(olap_product_data) == 5
```
Criamos uma função para nos auxiliar a invocar a execução da DAG durante o teste. Assim podemos executar novamente o `make testing`.
O teste irá retornar FAILED ainda com a diferença entre os dados. Porém se olharmos na interface do Airflow nossa DAG executou e foi um sucesso. O que falta agora é os dados dentro do banco oltp-db e eles irem para o banco olap-db. Essa etapa novamente não é responsabilidade da nossa DAG então ela deve ficar no setup do teste. Já temos o arquivo `products.csv` precisamos facilmente transportar ele para dentro do olap-db.

A forma mais simples que me vem na mente é ler o csv usando pandas e já salvar os dados no banco.

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

        olap_product_data = olap_hook.get_records(
            'select * from products'
        )
        assert len(olap_product_data) == 5
```

After this run again `make testing` AND OUR TEST PASSED!
```
======================== 1 passed, 1 warning in 11.06s =========================
```
The warning is from Airflow. You can check the output data using this  command:
```bash
docker exec -ti olap-db psql -U root olap
```
e depois `select * from products` teremos o seguinte resultado.
![Our first passed test output](../images/output_products.png)

Agora precisamos parar, pensar e respirar. Temos nosso teste passando. Porém o código que fizemos pode ser refatorado em algumas partes. Primeiro que temos uma outra tarefa muito parecida com a que executamos agora `load_incremental_transactions`.






---
### Some considerations

**Pq não usar um init.db?**
Acho que fazer a criação das tabelas e inserir os dados no proprio teste deixa bem explicito toda logica e dá uma flexibilidade muito grande. Pode existir cenários que você queira testar a DAG com a mesma tabela e porém com outro sample de dados, um cenário de validação por exemplo...

**Problemas ainda**
Não conseguimos testar a cada TASK executada. 

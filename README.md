# Airflow Testing with CI workflow

This code is complementar to my article [How to build a DataOps enviroment with Airflow (Part 1): setup CI/CD and DAG pipeline tests](https://medium.com/@marcosmarxm/how-to-build-a-dataops-enviroment-with-airflow-part-1-setup-ci-cd-and-dag-pipeline-tests-13cdf050439e).
I suggest you to read to better understand the code and the way I think how to setup the project.

## The project

Abaixo é representado de forma resumida o que será realizado nesse projeto. Simulamos a transferia de dados fictcios de transações de um ecommerce do banco source-db para o banco dest-db.

![Macro Worflow of the Project](./assets/images/macroflow.png)

Para auxiliar no desenvolvimento utilizamos um ambiente de desenvolvimento local e também integração CI com o Github Action. Abaixo segue a sequencia de execução do fluxo de operaçoes no projeto.

![Containers](./assets/images/localenvsetup.png)
- Airflow: container rodando setup local para dev;
- source-db e dest-db: container que simulam os bancos em ambiente de produçao e recebem os dados fake;
- handler: imagem python que possui scripts para carga inicial dos dados e também serve como "ambiente controlado" para testes.

### Dependencies?
Docker, docker-compose and makefile.

### How to run?

O comando abaixo irá subir o ambiente utilizando docker-compose. Aguardar alguns minutos (240s, yeah omg right?) para configuração correta do Airflow então criar as credenciais e conexões necessárias. Por ultimo realiza o insert dos dados iniciais nos bancos mock.
```
make setup
```
Rodando o comando acima é possivel acessar o Airflow no `localhost:8080`. É criado um usuario de test user:admin/password:admin. Nesse estagio você pode desenvolver suas DAGs e ir testando elas conforme for modificando.

O comando abaixo executa uma chamada para o Airflow rodar as DAGs que ja foram criadas e possuem testes. Caso você desenvolva uma nova DAGs você deve inserir ela aqui para quando rodar os testes e CI ela seja invocada.
```
make run
```

E por ultimo o comando que chama o container handler para executar os testes.
```
make tests
```
---

Some resources about Airflow testing and DataOps:
* [Pipelines on pipelines: Agile CI/CD workflows for Airflow DAGs @ Airflow Summit 2020](https://www.youtube.com/watch?v=tY4F9X5l6dg)
* [Data Testing with Airflow](https://github.com/danielvdende/data-testing-with-airflow)
* [Data's Inferno: 7 Circles of Data Testing Hell with Airflow](https://medium.com/wbaa/datas-inferno-7-circles-of-data-testing-hell-with-airflow-cef4adff58d8)
* [Testing and Debugging in Apache Airflow by GoDataDriven](https://godatadriven.com/blog/testing-and-debugging-apache-airflow/)
* [The Challenge of Testing Data Pipelines](https://medium.com/slalom-build/the-challenge-of-testing-data-pipelines-4450744a84f1)


# Teste Unilever

Estou anexando um projetinho em pyspark em 'projetoBase' e um jupyterNotebook que fiz os testes e execuções e suas evidências.

Achei confuso alguns pontos no enunciado. Como falaram que íamos conversar sobre o código, falarei sobre isso.

O projetoBase é iniciado executando a shell exec_workflow.sh no linux >>> ./exec_workflow.sh "executaClasse"
Ao ser iniciada, ela lê um arquivo de configuração para deixar a execução genérica e logo após executa o SparkSubmit.
Com isso, a Main do python é executada e a partir dela, com os parâmetros passados, executa a classe com a lógica dos CSV's, gerando ao final dos parquet.

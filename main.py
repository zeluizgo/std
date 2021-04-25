from Helpers.executionLogHelper import ExecutionLogHelper
from Helpers.sparkHelper import SparkHelper
from managerPagamentos import ManagerPagamentos

jobName: str = "Atualiza Pagamentos e Liquidacoes"
fileName: str = "main.py"
executionLogHelper = ExecutionLogHelper(jobName)

try:
  print('Processamento iniciado\n')

  spark = SparkHelper.getSparkSession()

  executionLogHelper.logJobBegin(spark, fileName, None)

  print('\nAtualizando Tabela de Pagamentos')
  managerPagamentos = ManagerPagamentos(spark, executionLogHelper)
  managerPagamentos.updatePagamentos()
  
except Exception as exception:
  print(f'==> ERRO: {exception}')
  executionLogHelper.logError(spark, fileName, exception)

finally:  
  executionLogHelper.logJobEnd(spark, fileName, None)

  SparkHelper.stopSparkSession(spark)

  print('\nProcessamento finalizado')
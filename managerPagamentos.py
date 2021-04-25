from Helpers.executionLogHelper import ExecutionLogHelper
from Helpers.sparkHelper import SparkHelper
from datetime import date, datetime, timedelta

class ManagerPagamentos:
  def __init__(self, spark, executionLogHelper):
    self.__fileName = "managerPagamentos.py"
    self.__spark = spark
    self.__executionLogHelper = executionLogHelper
    
  
  def __loadContratosAll(self, data_ugdtmae):    
    print ('INÍCIO - Carga de todos os contratos - UGDTMAE ...')
    
    if not data_ugdtmae.isSandboxUpToDate():      
      
      print ('Inserindo dados dos novos contratos na tabela da SandBox  ...')
      sql = 'insert into table sand_mdo_negocios.jl_analise_saldos_step1_v2 partition (o_dat_ref_carga)  ' 
      sql = sql + ' select entidad  , oficina   , cuenta    , producto  , subpro    , salteor, acuimpa,'
      sql = sql + ' sitpres , fevencin, fecsal, feuliq   , feproliq, fecpriliq, fecsitob, feforma, ugcdipag, impconce, cod_transfly, dat_ref_carga as o_dat_ref_carga'
      sql = sql + ' from ug.ugdtmae '
      sql = sql + ' where dat_ref_carga > "'
      sql = sql + data_ugdtmae.getSandboxDateAsStr()
      sql = sql + '" and subpro in ("6000","6001","6003","6004","6005","6006","6007","6008","6009") '
      sql = sql + ' and (sitpres=0 or (sitpres<> 0 and year(to_date(dat_ref_carga)) = year(to_date(fecsal)) '
      sql = sql + ' and month(to_date(dat_ref_carga)) = month(to_date(fecsal)) '
      sql = sql + ' )) '
      SparkHelper.executeQuery(self.__spark, self.__executionLogHelper,
        sql, self.__fileName, 'Carga de todos os contratos - UGDTMAE')

      data_ugdtmae.sandboxIsUpToDate()
    else:
      print ('Não fazendo nada ... data ref da tabela na sandbox (jl_analise_saldos_step1_v2) é igual a data data ref da tabela origem (ug.ugdtmae)')
      print (data_ugdtmae.getSandboxDateAsStr())
      print (data_ugdtmae.getOriginDateAsStr())
    
    print ('FIM - Carga de todos os contratos - UGDTMAE ...')
    
  def __loadContratosAtivos(self, data_ugdt243):
    print ('INÍCIO - Carga de todos os contratos Ativos - UGDT243 ...')
    if not data_ugdt243.isSandboxUpToDate():      
      print ('Inserindo dados da posição do dia dos contratos ativos na tabela da SandBox  ...')
  
      sql = ' insert into table sand_mdo_negocios.jl_analise_saldos_step2_v2 partition (dat_ref_carga)  '
      sql = sql + ' select cd_enti as entidad, cd_agen as oficina, cd_cntr as cuenta, '
      sql = sql + ' vl_sdev, vl_orig_parc, vl_prox_parc, dt_refe, VL_PARC_PEND,	DT_INIC_ATRS, dat_ref_carga '
      sql = sql + ' from ug.ugdt243  '
      sql = sql + ' where dat_ref_carga > "'
      sql = sql + data_ugdt243.getSandboxDateAsStr()
      sql = sql + '" and cd_subp in ("6000","6001","6003","6004","6005","6006","6007","6008","6009") '
      SparkHelper.executeQuery(self.__spark, self.__executionLogHelper,
        sql, self.__fileName, 'Carga de todos os contratos Ativos - UGDT243')
      
      data_ugdt243.sandboxIsUpToDate()      
    else:
      print ('Não fazendo nada ... data ref da tabela na sandbox (jl_analise_saldos_step2_v2) é igual a data data ref da tabela origem (ug.ugdt243)')
      print (data_ugdt243.getSandboxDateAsStr())
      print (data_ugdt243.getOriginDateAsStr())

    print ('FIM - Carga de todos os contratos ativos - UGDT243 ...')
    
  
  def __loadMovRecebimentos(self, data_ugdtmvr):
    print ('INÍCIO - Carga das movimentações de recebimento de contratos - UGDTMVR ...')
    if not data_ugdtmvr.isSandboxUpToDate():      
      print ('Inserindo dados das movimentações de recebimento de contratos na tabela da SandBox  ...')
      sql = ' insert into table sand_mdo_negocios.jl_analise_saldos_step4 partition (dat_ref_carga) '
      sql = sql + ' select ug01.entidad,  ug01.oficina, ug01.cuenta,  '
      sql = sql + ' max(ug01.feoper) as data_proc_pag,  '
      sql = sql + '  sum(case when ug01.ind_formpago = "F" then ug01.imp_pago else 0 end) as total_valor_pag,  '
      sql = sql + '  sum(case when ug01.ind_formpago = "F" then ug01.capinire else 0 end) as amortiz_pag,  '
      sql = sql + '  sum(case when ug01.ind_formpago = "F" then ug01.intinire else 0 end) as juros_pag,  '
      sql = sql + '  sum(case when ug01.ind_formpago = "F" then ug01.imp_mora else 0 end) as mora_pag, '
      sql = sql + '  sum(case when ug01.ind_formpago = "C" then ug01.imp_pago else 0 end) as total_valor_pag_contab,  '
      sql = sql + '  sum(case when ug01.ind_formpago = "C" then ug01.intinire else 0 end) as juros_pag_contab,  '
      sql = sql + '  sum(case when ug01.ind_formpago = "C" then ug01.imp_mora else 0 end) as mora_pag_contab, '
      sql = sql + '  sum(case when ug01.ind_formpago = "6" then ug01.imp_pago else 0 end) as total_valor_pag_ly, ug01.dat_ref_carga '
      sql = sql + ' from ug.ugdtmvr as ug01 '
      sql = sql + ' inner join sand_mdo_negocios.jl_analise_saldos_step1_v2 as ug02'
      sql = sql + ' where ug01.dat_ref_carga > "'
      sql = sql +       data_ugdtmvr.getSandboxDateAsStr()
      sql = sql + '" and ug01.dat_ref_carga = ug02.o_dat_ref_carga  '
      sql = sql + '  and ug01.entidad = ug02.o_entidad  '
      sql = sql + '  and ug01.oficina = ug02.o_oficina  '
      sql = sql + '  and ug01.cuenta = ug02.o_cuenta  '
      sql = sql + '  and ug01.ind_formpago in ("F", "C", "6") and ug01.indretro="N"  '
      sql = sql + ' group by ug01.cuenta, ug01.oficina, ug01.entidad, ug01.dat_ref_carga '
      SparkHelper.executeQuery(self.__spark, self.__executionLogHelper,
        sql, self.__fileName, 'Carga das movimentações de recebimento de contratos - UGDTMVR')
      
      data_ugdtmvr.sandboxIsUpToDate()
    else:
      print ('Não fazendo nada ... data ref da tabela na sandbox (jl_analise_saldos_step4) é igual a data data ref da tabela origem (ug.ugdtmvr)')
      print (data_ugdtmvr.getSandboxDateAsStr())
      print (data_ugdtmvr.getOriginDateAsStr())

    print ('FIM - Carga de todos os contratos - UGDTMVR ...')

    
  def __loadMovLiquidacoes(self, data_ugdtmov):
    print ('INÍCIO - Carga das movimentações de liquidação de contratos - UGDTMOV ...')
    if not data_ugdtmov.isSandboxUpToDate():      
      print ('Inserindo dados das movimentações de liquidação de contratos na tabela da SandBox  ...')
      sql = ' insert into table sand_mdo_negocios.jl_analise_saldos_step5 partition (dat_ref_carga) '
      sql = sql + ' select ug01.entidad,  ug01.oficina, ug01.cuenta, '
      sql = sql + ' max(ug01.feoper) as data_proc_liq,  '
      sql = sql + ' sum(case when substr(ug01.userid_umo,1,2) <> "UG" and substr(ug01.userid_umo,1,2) <> "ST" then ug01.imp_pago else 0 end) as total_valor_liq_robo,  '
      sql = sql + ' sum(case when ug01.codconli = "K01" and substr(ug01.userid_umo,1,2) <> "UG" and substr(ug01.userid_umo,1,2) <> "ST" then ug01.imp_pago else 0 end) as amortiz_liq_robo,   '
      sql = sql + ' sum(case when ug01.codconli = "101" and substr(ug01.userid_umo,1,2) <> "UG" and substr(ug01.userid_umo,1,2) <> "ST" then ug01.imp_pago else 0 end) as juros_liq_robo,   '
      sql = sql + ' sum(case when substr(ug01.userid_umo,1,2) = "UG" then ug01.imp_pago else 0 end) as total_valor_liq_cliente,  '
      sql = sql + ' sum(case when ug01.codconli = "K01" and substr(ug01.userid_umo,1,2) = "UG" then ug01.imp_pago else 0 end) as amortiz_liq_cliente,   '
      sql = sql + ' sum(case when ug01.codconli = "101" and substr(ug01.userid_umo,1,2) = "UG" then ug01.imp_pago else 0 end) as juros_liq_cliente ,  ug01.dat_ref_carga '
      sql = sql + ' from ug.ugdtmov as ug01 '
      sql = sql + ' inner join sand_mdo_negocios.jl_analise_saldos_step1_v2 as ug02'
      sql = sql + ' where ug01.dat_ref_carga > "'
      sql = sql + data_ugdtmov.getSandboxDateAsStr()
      sql = sql + '" and ug01.dat_ref_carga = ug02.o_dat_ref_carga  '
      sql = sql + '  and ug01.entidad = ug02.o_entidad  '
      sql = sql + '  and ug01.oficina = ug02.o_oficina  '
      sql = sql + '  and ug01.cuenta = ug02.o_cuenta  '
      sql = sql + '  and ug01.indretro="N"  '
      sql = sql + '  and ug01.cod_evento = "CAAN" and ug01.codconli in("K01","101") '
      sql = sql + ' group by ug01.cuenta, ug01.oficina, ug01.entidad, ug01.dat_ref_carga '
      SparkHelper.executeQuery(self.__spark, self.__executionLogHelper,
        sql, self.__fileName, 'Carga de todos os contratos - UGDTMOV')
      
      data_ugdtmov.sandboxIsUpToDate()
    else:
      print ('Não fazendo nada ... data ref da tabela na sandbox (jl_analise_saldos_step5) é igual a data data ref da tabela origem (ug.ugdtmov)')
      print (data_ugdtmov.getSandboxDateAsStr())
      print (data_ugdtmov.getOriginDateAsStr())

    print ('FIM - Carga de todos os contratos - UGDTMOV ...')
    
  def __loadPagamentos(self, data_ykmaculiq):
    print ('INÍCIO - Carga dos pagamentos - YKMACULIQ ...')
    if not data_ykmaculiq.isSandboxUpToDate():        
      print ('Inserindo dados pagamentos de contratos na tabela da SandBox  ...')
      sql = ' insert into table sand_mdo_negocios.jl_analise_saldos_step6 partition (dat_ref_carga) '
      sql = sql + ' select  "0033" as entidad,   substr(mvpr2_seunumer,-4) as oficina,  '
      sql = sql + ' concat("3",substr(mvpr2_seunumer,1,11)) as cuenta,  '
      sql = sql + ' mvpr2_cd_ident_psk		,mvpr2_cd_ident_ttk		,mvpr2_cd_ident_tek		, '
      sql = sql + ' mvpr2_cd_seq_esk		,mvpr2_codagmov		,mvpr2_codpvmov		,mvpr2_datamovi		, '
      sql = sql + ' mvpr2_ctaempce		,mvpr2_origem		,mvpr2_seunumer		,mvpr2_datavenc		, '
      sql = sql + ' mvpr2_valortit		,mvpr2_vljrperm		,mvpr2_vltotrec		,mvpr2_valorliq		, '
      sql = sql + ' mvpr2_vlprmdiv		,mvpr2_ctacredi		,mvpr2_datacred		,mvpr2_datarece		, '
      sql = sql + ' mvpr2_tx_descr_ttk		,mvpr2_valorli1		,mvpr2_cpfcgsac		,dat_ref_carga	 '
      sql = sql + ' from yk.ykmaculiq  '
      sql = sql + ' where dat_ref_carga > "'
      sql = sql + data_ykmaculiq.getSandboxDateAsStr()
      sql = sql + '"  and mvpr2_cd_ident_psk = 1977946 and mvpr2_cd_ident_tek < 7800000000  '
      SparkHelper.executeQuery(self.__spark, self.__executionLogHelper,
        sql, self.__fileName, 'Carga dos pagamentos - YKMACULIQ')
    
      data_ykmaculiq.sandboxIsUpToDate()
      print ('Obtendo o dia útil recem carregado...: DIAS_UTEIS_2020 ...')
      sql = ' select dia_util_mes from sand_mdo_negocios.dias_uteis '
      sql = sql + ' where dia_util_data_ref = "'
      sql = sql + data_ykmaculiq.getOriginDateAsStr()
      sql = sql + '"'
      print(sql)


      df = SparkHelper.executeQuery(self.__spark, self.__executionLogHelper,
        sql, self.__fileName, 'Obtendo o dia útil - DIAS_UTEIS_2020')

      for key, value in df.first().asDict().items():
          globals()[key] = value

      print (dia_util_mes)

      
      SparkHelper.dropTable(self.__spark, self.__executionLogHelper,
        'jl_analise_pagamentos_step1', self.__fileName, None)

      print ('Obtendo as datas referência dos meses anteriores para o mesmo dia util atual...')

      sql = ' create table sand_mdo_negocios.jl_analise_pagamentos_step1 '
      sql = sql + ' select  ano, dia_util_data_ref, mes, dia_util_mes  '
      sql = sql + ' from sand_mdo_negocios.dias_uteis  '
      sql = sql + ' where dia_util_data_ref <= "'
      sql = sql + data_ykmaculiq.getOriginDateAsStr()
      sql = sql + '"  and dia_util_mes = '
      sql = sql + str(dia_util_mes)
      sql = sql + ' '
      SparkHelper.executeQuery(self.__spark, self.__executionLogHelper,
        sql, self.__fileName, 'Obtendo as datas referência')


      SparkHelper.dropTable(self.__spark, self.__executionLogHelper,
        'jl_analise_pagamentos_step2', self.__fileName, None)

      print ('Listando pagamentos até o dia util atual...')
      sql = ' create table sand_mdo_negocios.jl_analise_pagamentos_step2 as '
      sql = sql + ' select  ap01.ano, ap01.mes,  as01.entidad, as01.oficina, as01.cuenta,  max(as01.mvpr2_datamovi) as data_pgto, '
      sql = sql + '   max(as01.mvpr2_cd_ident_tek) as cod_evento, sum(as01.mvpr2_valortit) as valor_titulo_pago	'
      sql = sql + ' from sand_mdo_negocios.jl_analise_pagamentos_step1  as ap01 '
      sql = sql + ' inner join sand_mdo_negocios.jl_analise_saldos_step6 as as01  '
      sql = sql + ' on as01.dat_ref_carga <= ap01.dia_util_data_ref  '
      sql = sql + ' and month(to_date(as01.dat_ref_carga)) =  ap01.mes '
      sql = sql + ' and year(to_date(as01.dat_ref_carga)) =  ap01.ano '
      sql = sql + ' group by  ap01.ano, ap01.mes, as01.entidad, as01.oficina, as01.cuenta '
      sql = sql + ' '
      SparkHelper.executeQuery(self.__spark, self.__executionLogHelper,
        sql, self.__fileName, 'Listando pagamentos até o dia')


      SparkHelper.dropTable(self.__spark, self.__executionLogHelper,
        'jl_analise_pagamentos_step3', self.__fileName, None)
      
      
      print ('Listando Liquidações de LY até o dia util atual...')
      sql = ' create table sand_mdo_negocios.jl_analise_pagamentos_step3 as '
      sql = sql + ' select  ap01.ano, ap01.mes,  as01.entidad, as01.oficina, as01.cuenta,  max(as01.data_proc_pag) as data_ly, '
      sql = sql + ' sum(as01.total_valor_pag_ly) as valor_ly	'
      sql = sql + ' from sand_mdo_negocios.jl_analise_pagamentos_step1  as ap01 '
      sql = sql + ' inner join sand_mdo_negocios.jl_analise_saldos_step4 as as01  '
      sql = sql + ' on as01.dat_ref_carga <= ap01.dia_util_data_ref  '
      sql = sql + ' and month(to_date(as01.dat_ref_carga)) =  ap01.mes '
      sql = sql + ' and year(to_date(as01.dat_ref_carga)) =  ap01.ano '
      sql = sql + ' and as01.total_valor_pag_ly > 0 '
      sql = sql + ' group by  ap01.ano, ap01.mes, as01.entidad, as01.oficina, as01.cuenta '
      sql = sql + ' '
      SparkHelper.executeQuery(self.__spark, self.__executionLogHelper,
        sql, self.__fileName, 'Listando Liquidações de LY até o dia')

      SparkHelper.dropTable(self.__spark, self.__executionLogHelper,
        'jl_analise_pagamentos_step4', self.__fileName, None)

      print ('Listando Liquidações de contrato via Robô até o dia util atual...')
      sql = ' create table sand_mdo_negocios.jl_analise_pagamentos_step4 as '
      sql = sql + ' select  ap01.ano, ap01.mes,  as01.entidad, as01.oficina, as01.cuenta,  max(as01.data_proc_liq) as data_liq, '
      sql = sql + ' sum(as01.total_valor_liq_robo) as valor_liq	'
      sql = sql + ' from sand_mdo_negocios.jl_analise_pagamentos_step1  as ap01 '
      sql = sql + ' inner join sand_mdo_negocios.jl_analise_saldos_step5 as as01  '
      sql = sql + ' on as01.dat_ref_carga <= ap01.dia_util_data_ref  '
      sql = sql + ' and month(to_date(as01.dat_ref_carga)) =  ap01.mes '
      sql = sql + ' and year(to_date(as01.dat_ref_carga)) =  ap01.ano '
      sql = sql + ' and as01.total_valor_liq_robo > 0  '
      sql = sql + ' group by  ap01.ano, ap01.mes, as01.entidad, as01.oficina, as01.cuenta '
      sql = sql + ' '
      SparkHelper.executeQuery(self.__spark, self.__executionLogHelper,
        sql, self.__fileName, 'Listando Liquidações via Robôs até o dia')

      SparkHelper.dropTable(self.__spark, self.__executionLogHelper,
        'jl_analise_pagamentos_step5', self.__fileName, None)

      sql = f''' 
      create table sand_mdo_negocios.jl_analise_pagamentos_step5 (
      entidad string,
      oficina string,
      cuenta string,
      nr_matr string, 
      nome_func string,
      uniorg string, 
      filial string,
      regional string,
      vcto_parcela string,
      valor_parcela decimal(19,4),
      data_pgto string,
      ugcdipag string,
      dia_util_mes int,
      status_evento string,
      valor_pago_evento decimal(19,4)) partitioned by (ano int, mes int)
      '''

      SparkHelper.executeQuery(self.__spark, self.__executionLogHelper,
        sql, self.__fileName, 'Criando tabela de visão geral')

      print ('Montando tabela de visão geral até o dia util atual...')
      sql = ' insert into table sand_mdo_negocios.jl_analise_pagamentos_step5 partition (ano, mes)  '
      sql = sql + ' select  as01.entidad, as01.oficina, as01.cuenta, as01.nr_matr,  as01.nome_func,  as01.uniorg,  as01.filial,  as01.regional, as01.vcto_parcela, as01.valor_parcela, ap01.data_pgto, as01.ugcdipag, ap04.dia_util_mes, '
      sql = sql + ' case when not ap01.cuenta is null then yk01.grupo_evento	'
      sql = sql + '      when not ap02.cuenta is null then "LY"	'
      sql = sql + '      when not ap03.cuenta is null then "Revitalizacao"	'
      sql = sql + '      when datediff(to_date(as01.vcto_parcela), to_date(ap04.dia_util_data_ref))<0 then "Inadimplente"	'
      sql = sql + '      else  "Pendente" end as status_evento,	'
      sql = sql + ' case when not ap01.cuenta is null then ap01.valor_titulo_pago	'
      sql = sql + '      when not ap02.cuenta is null then as01.valor_parcela	'
      sql = sql + '      when not ap03.cuenta is null then as01.valor_parcela	'
      sql = sql + '      else  0 end as valor_pago_evento, as01.ano, as01.mes	'
      sql = sql + ' from sand_mdo_negocios.jl_analise_saldos_step8_estoque_v2  as as01 '
      sql = sql + ' left join sand_mdo_negocios.jl_analise_pagamentos_step2 as ap01  '
      sql = sql + ' on as01.ano = ap01.ano  '
      sql = sql + ' and as01.mes = ap01.mes '
      sql = sql + ' and as01.entidad = ap01.entidad  '
      sql = sql + ' and as01.oficina = ap01.oficina  '
      sql = sql + ' and as01.cuenta = ap01.cuenta  '
      sql = sql + ' left join sand_mdo_negocios.jl_eventos_ykmaculiq as yk01  '
      sql = sql + ' on ap01.cod_evento = yk01.cod_evento  '
      sql = sql + ' left join sand_mdo_negocios.jl_analise_pagamentos_step3 as ap02  '
      sql = sql + ' on as01.ano = ap02.ano  '
      sql = sql + ' and as01.mes = ap02.mes '
      sql = sql + ' and as01.entidad = ap02.entidad  '
      sql = sql + ' and as01.oficina = ap02.oficina  '
      sql = sql + ' and as01.cuenta = ap02.cuenta  '
      sql = sql + ' left join sand_mdo_negocios.jl_analise_pagamentos_step4 as ap03  '
      sql = sql + ' on as01.ano = ap03.ano  '
      sql = sql + ' and as01.mes = ap03.mes '
      sql = sql + ' and as01.entidad = ap03.entidad  '
      sql = sql + ' and as01.oficina = ap03.oficina  '
      sql = sql + ' and as01.cuenta = ap03.cuenta  '
      sql = sql + ' left join sand_mdo_negocios.jl_analise_pagamentos_step1 as ap04  '
      sql = sql + ' on as01.ano = ap04.ano  '
      sql = sql + ' and as01.mes = ap04.mes '
      SparkHelper.executeQuery(self.__spark, self.__executionLogHelper,
        sql, self.__fileName, 'Montando tabela de visão geral')

      sql = 'select ano, mes, max(data_pgto) as data_ref, min(vcto_parcela) as vcto_de, max(vcto_parcela) as vcto_ate, count(cuenta) as qtde_conrtatos, sum(valor_parcela) as estoque, sum(case when status_evento = "Pendente" then 0 else 1 end) as qtde_contratos_pagos, sum(case when status_evento = "Pendente" then 0 else valor_pago_evento end) as valor_contratos_pagos  from sand_mdo_negocios.jl_analise_pagamentos_step5 group by ano, mes order by ano, mes'
      
      SparkHelper.executeQuery(self.__spark, self.__executionLogHelper,
        sql, self.__fileName, 'Resumindo tabela de visão geral').show()

    else:
      print ('Não fazendo nada ... data ref da tabela na sandbox (jl_analise_saldos_step6 é igual a data data ref da tabela origem (yk.ykmaculiq) ')
      print (data_ykmaculiq.getSandboxDateAsStr())
      print (data_ykmaculiq.getOriginDateAsStr())

    print ('FIM - Carga de todos os contratos - YKMACULIQ ...')     
  
  def __loadPagamentosFiltroQlik(self, data_ykmaculiq):
    print('\nAtualizando tabela analise_pagamentos_step5_filter_status para uso no Qlik')
    
    if data_ykmaculiq.isSandboxUpToDate():  
      SparkHelper.dropTable(self.__spark, self.__executionLogHelper,
        'analise_pagamentos_step5_filter_status', self.__fileName, None)
      
      currentMonth = date.today().month
      
      sql = f'''
        CREATE TABLE sand_mdo_negocios.analise_pagamentos_step5_filter_status AS
        SELECT
                        COALESCE(nome_func, '-') AS func_nome,
                        COALESCE(filial, '-') AS filial,
                        COALESCE(regional, '-') AS regional,
                        case when          
                            status_evento not in ('Pendente', 'Revitalizacao', 'LY', 'Inadimplente') Then 'Pago'
                            when status_evento = 'Pendente' then 'Pendente'
                            when status_evento = 'Revitalizacao' then 'Revitalizacao'
                            when status_evento = 'LY' then 'LY'
                when status_evento = 'Inadimplente' then 'Inadimplente'
                        end as status
        FROM sand_mdo_negocios.jl_analise_pagamentos_step5
        where mes = "{currentMonth}"
      '''
      
      SparkHelper.executeQuery(self.__spark, self.__executionLogHelper,
        sql, self.__fileName, 'Resumindo tabela de visão geral filtro qlik')
    else:
      print ('Não fazendo nada ... data ref da tabela na sandbox (jl_analise_saldos_step6) não é igual a data data ref da tabela origem (yk.ykmaculiq) ')
      print (data_ykmaculiq.getSandboxDateAsStr())
      print (data_ykmaculiq.getOriginDateAsStr())
  
  def updatePagamentos(self):
    
    self.__spark.sql( "set hive.exec.dynamic.partition.mode=nonstrict");
    
    data_ugdtmae = SparkHelper.validateSandboxTableIsUpToDateByQueryAndPartition(
      self.__spark, self.__executionLogHelper, self.__fileName, 
      'dat_ref_carga', 'ug.ugdtmae',
      'o_dat_ref_carga ', 'jl_analise_saldos_step1_v2', None)
    
    self.__loadContratosAll(data_ugdtmae)

    data_ugdt243 = SparkHelper.validateSandboxTableIsUpToDateByQueryAndPartition(
      self.__spark, self.__executionLogHelper, self.__fileName, 
      'dat_ref_carga', 'ug.ugdt243',
      'dat_ref_carga ', 'jl_analise_saldos_step2_v2', None)
      
    self.__loadContratosAtivos(data_ugdt243)
    
    data_ugdtmvr = SparkHelper.validateSandboxTableIsUpToDateByQueryAndPartition(
      self.__spark, self.__executionLogHelper, self.__fileName, 
      'dat_ref_carga', 'ug.ugdtmvr',
      'dat_ref_carga ', 'jl_analise_saldos_step4', None)
    
    self.__loadMovRecebimentos(data_ugdtmvr)

    data_ugdtmov = SparkHelper.validateSandboxTableIsUpToDateByQueryAndPartition(
      self.__spark, self.__executionLogHelper, self.__fileName, 
      'dat_ref_carga', 'ug.ugdtmov',
      'dat_ref_carga ', 'jl_analise_saldos_step5', None)
      
    self.__loadMovLiquidacoes(data_ugdtmov)

    data_ykmaculiq = SparkHelper.validateSandboxTableIsUpToDateByQueryAndPartition(
      self.__spark, self.__executionLogHelper, self.__fileName, 
      'dat_ref_carga', 'yk.ykmaculiq',
      'dat_ref_carga ', 'jl_analise_saldos_step6', None)
      
    self.__loadPagamentos(data_ykmaculiq)
    self.__loadPagamentosFiltroQlik(data_ykmaculiq)
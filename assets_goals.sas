/*   START OF NODE: HistÛrico de Saldo MÈdio de Ativos do Painel   */
%LET _CLIENTTASKLABEL='HistÛrico de Saldo MÈdio de Ativos do Painel';
%LET _CLIENTPROJECTPATH='C:\Users\T694592\Desktop\working_sas\Metas_Saldos_GR_v2.egp';
%LET _CLIENTPROJECTNAME='Metas_Saldos_GR_v2.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;
/*LIBNAME KODAMA  '/user/user_info/KODAMA';*/
LIBNAME CADASTRO ORACLE USER = INCENTIVOS PW = 'cbrimincentivos052015' PATH = 'ORAPR072' SCHEMA = CADASTRO;

/* Base de saldos somente rede Padr„o!! */
PROC SQL;
   CREATE TABLE JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_PADRAO AS 
   SELECT t1.CD_PERI, 
   		  t1.TP_PESS,
          t1.CD_SEGM, 
          t1.CD_AGEN_VF as CD_AGEN, 
          t1.CD_MATR_VF as CD_MATR, 
          t1.CAT_PROD,
		  case when t1.CD_PVEN_VF eq . then t1.CD_AGEN_VF else t1.CD_PVEN_VF end as CD_PVEN_VF2,
		  "PADRAO" AS TIPO_REDE,
            (SUM(t1.VL_SALD_MEDI_M0_ATIV))     AS VL_SALD_MEDI_M0,
                (SUM(t1.VL_SALD_MEDI_M1_ATIV)) AS VL_SALD_MEDI_M1,
                (SUM(t1.VL_NOVA_CNTR))              AS VL_ENTRADA_NOVA,
                (SUM(t1.VL_RNVC_CNTR))              AS VL_ENTRADA_EXIS,
                (SUM(t1.VL_LIQU_PARC))               AS VL_SAIDA_PARC,
               (SUM(t1.VL_QUIT_CNTR))               AS VL_SAIDA_TOT,
                (SUM(t1.GANHO_MATR_VF_MIGR))   AS GANHO_MATR_MIGR,
                (SUM(t1.PERDA_MATR_VF_MIGR))   AS PERDA_MATR_MIGR,
                (SUM(t1.DELT_SALD_MEDI_ATIV)) AS DELT_SALD_MEDI

      FROM CADASTRO.TB_PNEL_CERT_SALDOS_SUM_ATI_PR t1
       where t1.CD_MATR_VF > 0 and t1.CD_PERI in (201701,201702,201703) and t1.CD_SEGM <> '7'
      GROUP BY t1.CD_PERI,
   		       t1.TP_PESS,
               t1.CD_SEGM,
               t1.CD_AGEN_VF,
               t1.CD_MATR_VF,
               t1.CAT_PROD,
          	   CD_PVEN_VF2,
			   TIPO_REDE
          ;
QUIT;

/* Base de saldos somente rede Remoto!! */
PROC SQL;
   CREATE TABLE JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_REMOTO AS 
   SELECT t1.CD_PERI, 
   		  t1.TP_PESS,
          t1.CD_SEGM, 
          t1.CD_AGEN_REMO as CD_AGEN,  
          t1.CD_MATR_REMO as CD_MATR, 
          t1.CAT_PROD, 
         /* t1.CD_PVEN_VF, */
		  "REMOTO" AS TIPO_REDE,
            (SUM(t1.VL_SALD_MEDI_M0_ATIV))     AS VL_SALD_MEDI_M0,
                (SUM(t1.VL_SALD_MEDI_M1_ATIV)) AS VL_SALD_MEDI_M1,
                (SUM(t1.VL_NOVA_CNTR))              AS VL_ENTRADA_NOVA,
                (SUM(t1.VL_RNVC_CNTR))              AS VL_ENTRADA_EXIS,
                (SUM(t1.VL_LIQU_PARC))               AS VL_SAIDA_PARC,
                (SUM(t1.VL_QUIT_CNTR))               AS VL_SAIDA_TOT,
                (SUM(t1.GANHO_MATR_REMO_MIGR))   AS GANHO_MATR_MIGR,
                (SUM(t1.PERDA_MATR_REMO_MIGR))   AS PERDA_MATR_MIGR,
                (SUM(t1.DELT_SALD_MEDI_ATIV)) AS DELT_SALD_MEDI


      FROM CADASTRO.TB_PNEL_CERT_SALDOS_SUM_ATI_PR  t1
       where t1.CD_MATR_REMO > 0 and t1.CD_PERI in (201701,201702,201703) and t1.CD_SEGM <> '7'
      GROUP BY t1.CD_PERI,
   		       t1.TP_PESS,
               t1.CD_SEGM,
               t1.CD_AGEN_REMO,
               t1.CD_MATR_REMO,
               t1.CAT_PROD,
          	  /* t1.CD_PVEN_VF, */
			   TIPO_REDE
;
QUIT;


data joseluiz.tb_aux_de_para_cdniorg_ag;
set incent.tb_mt_estrutura (where=(NR_VERSAO = 1 and NR_ANO = 2017 and NR_TRIMESTRE = 3 and CD_ATAIR ne 0)
keep=UNIORG UNIORG_AG STATUS_META CD_ATAIR NR_VERSAO NR_ANO NR_TRIMESTRE);
rename UNIORG_AG=CD_AGEN CD_ATAIR=CD_PVEN_VF;
drop STATUS_META NR_VERSAO NR_ANO NR_TRIMESTRE;
run;

data joseluiz.tb_aux_de_para_altair;
set incent.tb_mt_estrutura (where=(NR_VERSAO = 1 and NR_ANO = 2017 and NR_TRIMESTRE = 3 and STATUS_META= "INDEPENDENTE" and CD_ATAIR > 6000)
keep=UNIORG UNIORG_AG STATUS_META CD_ATAIR NR_VERSAO NR_ANO NR_TRIMESTRE);
UNIORG_AG = CD_ATAIR;
rename UNIORG_AG=CD_AGEN CD_ATAIR=CD_PVEN_VF;
drop STATUS_META NR_VERSAO NR_ANO NR_TRIMESTRE;
run;

data    JOSELUIZ.tb_aux_de_para_cdniorg_ag_altair; 
        set JOSELUIZ.tb_aux_de_para_cdniorg_ag 
                JOSELUIZ.tb_aux_de_para_altair; 
run;
/*
data JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_PADRAO ; 
set JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_PADRAO ;
		if CD_PVEN_VF eq . then CD_PVEN_VF = CD_AGEN;
run;
*/
proc sort data=JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_PADRAO;
	by CD_AGEN CD_PVEN_VF2;
run;

proc sort data=joseluiz.tb_aux_de_para_cdniorg_ag_altair;
	by CD_AGEN CD_PVEN_VF;
run;


data    JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_PADRAO;
	merge JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_PADRAO (IN=A rename=CD_PVEN_VF2=CD_PVEN_VF)
		  joseluiz.tb_aux_de_para_cdniorg_ag_altair (IN=B);
	by CD_AGEN CD_PVEN_VF;
	if A and B;
run;

/* Ajuste MatrÌcula Carteira Compartilhada */
data JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_PADRAO;
set JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_PADRAO;
if CD_MATR = 7251403 and UNIORG < 20000 then CD_MATR = 99990000000 + UNIORG;
if CD_MATR = 7251403 and UNIORG > 20000 then CD_MATR = 99980000000 + UNIORG;
run;


proc sort data=JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_REMOTO ;
	by CD_AGEN ;
run;

proc sort data=joseluiz.tb_aux_de_para_altair;
	by CD_AGEN ;
run;


data    JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_REMOTO;
	merge JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_REMOTO (IN=A)
		  joseluiz.tb_aux_de_para_altair (IN=B);
	by CD_AGEN ;
	if A and B;
run;



/* Base de Saldos Total da Rede */
data    JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_REDE; 
        set JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_PADRAO 
                JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_REMOTO; 
CD_AGEN = UNIORG;
drop CD_PVEN_VF UNIORG;
if CD_SEGM = '009' then
do;
	TP_PESS = '1';
	CD_SEGM = '1';
end;
if CD_SEGM = '001' or CD_SEGM = '002' or CD_SEGM = '003' or CD_SEGM = '304'  then
do;
	TP_PESS = '1';
	CD_SEGM = '2';
end;
if CD_SEGM = '004' or CD_SEGM = '005' or CD_SEGM = '006' or CD_SEGM = '007' or CD_SEGM = '106'  then
do;
	TP_PESS = '1';
	CD_SEGM = '3';
end;

if CD_SEGM = '011' or CD_SEGM = '015' or CD_SEGM = '226' or CD_SEGM = '227'  then
do;
	TP_PESS = '2';
	CD_SEGM = '5';
end;

if CD_SEGM = '012' or CD_SEGM = '229' then
do;
	TP_PESS = '2';
	CD_SEGM = '6';
end;

if CD_SEGM = '228' then
do;
	TP_PESS = '2';
	CD_SEGM = '7';
end;

/* CorreÁ„o p/ SMart Red */
if CD_AGEN = 11837	then CD_AGEN = 11699;
if CD_AGEN = 11838	then CD_AGEN = 14682;
if CD_AGEN = 11839	then CD_AGEN = 13463;
if CD_AGEN = 11843	then CD_AGEN = 10090;
if CD_AGEN = 11832	then CD_AGEN = 13563;
if CD_AGEN = 11841	then CD_AGEN = 10112;
if CD_AGEN = 11840	then CD_AGEN = 10154;
if CD_AGEN = 11842	then CD_AGEN = 10044;
if CD_AGEN = 11844	then CD_AGEN = 13972;
if CD_AGEN = 11836	then CD_AGEN = 14325;
if CD_AGEN = 11845	then CD_AGEN = 13330;

run;


GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;


/*   START OF NODE: C·lculo de Baixas e Accrual das Carteiras   */
%LET _CLIENTTASKLABEL='C·lculo de Baixas e Accrual das Carteiras';
%LET _CLIENTPROJECTPATH='C:\Users\T694592\Desktop\working_sas\Metas_Saldos_GR_v2.egp';
%LET _CLIENTPROJECTNAME='Metas_Saldos_GR_v2.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;


proc sort data=JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_REDE ;
	by CD_PERI CAT_PROD CD_AGEN CD_MATR TP_PESS CD_SEGM;
run;

proc sort data=joseluiz.tb_aux_bktest_meta_prod_gr;
	by CD_PERI CAT_PROD CD_AGEN CD_MATR TP_PESS CD_SEGM;
run;


data    JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_REDE;
	merge JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_REDE (IN=A)
		  joseluiz.tb_aux_bktest_meta_prod_gr (IN=B);
	by CD_PERI CAT_PROD CD_AGEN CD_MATR TP_PESS CD_SEGM;
	if A;
run;


/* Calculo de Baixas + Accruals (Perdas) */
data joseluiz.tb_aux_calculo_baixas_ativos;
set JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_REDE;
AUX_PERDA = 1.0;
/* Calculando Perda (Baixas + Accrual)...*/
if VL_SALD_MEDI_M1 ne 0.0 then AUX_PERDA = (VL_SALD_MEDI_M0-VL_SALD_MEDI_M1-VL_ENTRADA_NOVA-VL_ENTRADA_EXIS)/VL_SALD_MEDI_M1;

/* Excluindo Outlier ...*/
if CAT_PROD ne "ANTECIPACAO DE RECEBIVEIS" 
	and CAT_PROD ne "DESCONTO DE CHEQUES/TITULOS" 
	and AUX_PERDA < -0.19 then delete;

/* Excluindo Outlier ...*/
if (CAT_PROD eq "ANTECIPACAO DE RECEBIVEIS" 
	or CAT_PROD eq "DESCONTO DE CHEQUES/TITULOS")
	and AUX_PERDA < -0.79 then delete;
if AUX_PERDA >= 0 then delete;
run;

/* Estabelecendo limites Minimos e M·ximos de acordo com o histÛrico */
/* Calculando desvio padr„o e mÈdias ..*/
proc SQL;
create table joseluiz.TB_JL_PARAM_LIM_BAIXAS_ATIVOS as
select CAT_PROD, TP_PESS, CD_SEGM,
/* Para SofisticaÁ„o no futuro:  TIPO_REDE */
MIN(CD_PERI) as HIST_INICIO,
MAX(CD_PERI) as HIST_FIM,
AVG(AUX_PERDA) as MEDIA,
STD(AUX_PERDA) as DESVIO
from joseluiz.tb_aux_calculo_baixas_ativos
group by CAT_PROD, TP_PESS, CD_SEGM;
quit;
/* Finalizando tabela com limites minimos e m·ximo por segmento e categoria de produto */
data joseluiz.TB_JL_PARAM_LIM_BAIXAS_ATIVOS;
set joseluiz.TB_JL_PARAM_LIM_BAIXAS_ATIVOS;
LIMITE_MIN = MEDIA - DESVIO/2;
LIMITE_MAX = MEDIA + DESVIO/2;
run;

/* Aplicando limites mÌnimos e m·ximos ‡ performance da Rede no histÛrico (forÁando performance dentro de limites aceit·veis)...*/
proc sql;
create table joseluiz.tb_aux_calcula_baixas_ativos_gr as
select t1.CAT_PROD, t1.TP_PESS, t1.CD_SEGM, t1.CD_AGEN, t1.CD_MATR, 
		AVG(case when t1.aux_perda < t2.LIMITE_MIN then t2.MEDIA 
	         when t1.aux_perda > t2.LIMITE_MAX then t2.LIMITE_MAX
	        else t1.aux_perda end) as AUX_PERDA2
from joseluiz.tb_aux_calculo_baixas_ativos as t1 
	inner join joseluiz.TB_JL_PARAM_LIM_BAIXAS_ATIVOS as t2
	on t1.CAT_PROD = t2.CAT_PROD
		and t1.TP_PESS = t2.TP_PESS
		and t1.CD_SEGM = t2.CD_SEGM
group by t1.CAT_PROD, t1.TP_PESS, t1.CD_SEGM, t1.CD_AGEN, t1.CD_MATR;
quit;


GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;


/*   START OF NODE: Metas ProduÁ„o GR e somente AG   */
%LET _CLIENTTASKLABEL='Metas ProduÁ„o GR e somente AG';
%LET _CLIENTPROJECTPATH='C:\Users\T694592\Desktop\working_sas\Metas_Saldos_GR_v2.egp';
%LET _CLIENTPROJECTNAME='Metas_Saldos_GR_v2.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;


/* DE-PARA de Categoria de Gerentes para os segmentos do Painel de Saldos...*/
data joseluiz.tb_aux_de_para_categ_gr_segm_pnl;
set incent.TB_SDM_BASE_GR (where=(NR_ANO=2017 and NR_TRIMESTRE = 2 and NR_MES = 1 ) /* back test: NR_TRIMESTRE = 1 and NR_MES = 1 */
 keep= MATRICULA CD_FINAL CD_CATEGORIA NM_CATEGORIA STATUS TIPO NR_ANO NR_TRIMESTRE NR_MES);
CD_SEGM = '0';
TIPO_REDE = "PADRAO";
TP_PESS = 1;
if find(NM_CATEGORIA, 'VG','i') ge 1 or find(NM_CATEGORIA, 'VAN GOGH','i') ge 1 then CD_SEGM = '2';
if find(NM_CATEGORIA, 'CARTEIRA COMPARTILHADA','i') ge 1 then CD_SEGM = '3';
if find(NM_CATEGORIA, 'SELECT','i') ge 1  or find(NM_CATEGORIA, 'SMART RED','i') ge 1 then CD_SEGM = '1';

if find(NM_CATEGORIA, 'EI DIGITAL','i') ge 1  or find(NM_CATEGORIA, 'EMPRESAS I','i') ge 1 or 
   find(NM_CATEGORIA, 'NEGOCIOS','i') ge 1  or find(NM_CATEGORIA, 'EXPANSAO','i') ge 1 then CD_SEGM = '5';

if find(NM_CATEGORIA, 'TEAM LEADER','i') ge 1  or find(NM_CATEGORIA, 'EMPRESAS II','i') ge 1 or 
   find(NM_CATEGORIA, 'POLO','i') ge 1 then CD_SEGM = '6';
   
if find(NM_CATEGORIA, 'N?CLEO','i') ge 1 then CD_SEGM = '7';

if find(NM_CATEGORIA, 'DIGITAL','i') ge 1 then TIPO_REDE = "REMOTO";

if CD_SEGM eq '5'  or CD_SEGM eq '6' or CD_SEGM eq '7' then TP_PESS = 2;
/* para fins de backtest: */
MATRICULA_TEXTO = strip(put(MATRICULA, 20.0));
run;

/* Selecionando Metas de produÁ„o por GR para frente */
proc sql;
create table joseluiz.tb_aux_meta_prod_gr as
select t1.MATRICULA as CD_MATR, t4.UNIORG as CD_AGEN, 
		t2.BLOCO_SALDO as CAT_PROD, t3.CD_SEGM, t3.TIPO_REDE, t3.TP_PESS,
		sum(t1.M1*1000) as META_PROD_M1, SUM(t1.M2*1000) as META_PROD_M2
from incent.tb_sdm_meta_gr_2017 as t1 
	inner join (SELECT DISTINCT BLOCO_SALDO, CODIGO_SDM
FROM joseluiz.TB_JL_DE_PARA_PROD_SALDOS) as t2
	on t1.CD_SUBPRODUTO = t2.CODIGO_SDM
	inner join joseluiz.tb_aux_de_para_categ_gr_segm_pnl as t3
	on t1.MATRICULA = t3.MATRICULA and t1.CD_FINAL_PARA = t3.CD_FINAL
	inner join incent.tb_mt_estrutura as t4
	on t1.CD_FINAL_PARA = t4.CD_FINAL
where t1.NR_TRIMESTRE = 2  and t1.NR_ANO = 2017
and t4.NR_VERSAO = 1 and t4.NR_ANO = 2017 and t4.NR_TRIMESTRE = 3
group by CD_MATR, CD_AGEN, 
	CAT_PROD, t3.CD_SEGM, t3.TIPO_REDE, t3.TP_PESS;
quit;


/* Separando Metas de ProduÁ„o do GG vs somente GR */
proc sql;
create table joseluiz.tb_aux_meta_prod_ag as
select t4.UNIORG as CD_AGEN, 
		t2.BLOCO_SALDO as CAT_PROD, 
		case when DS_SEGMENTO = 'SELECT' then '1' 
			when DS_SEGMENTO LIKE 'VAN GOGH%' then '2'
			when DS_SEGMENTO = 'PF' then '3'
			else '5' end as CD_SEGM, "PADRAO" as TIPO_REDE, 
		case when CD_SEGMENTO = 4 then 2 else 1 end AS TP_PESS,
		sum(t1.M1*1000) as META_PROD_M1, SUM(t1.M2*1000) as META_PROD_M2
from incent.tb_sdm_meta_ag_2017 as t1 
	inner join (SELECT DISTINCT BLOCO_SALDO, CODIGO_SDM
FROM joseluiz.TB_JL_DE_PARA_PROD_SALDOS) as t2
	on t1.CD_SUBPRODUTO = t2.CODIGO_SDM
	inner join incent.tb_mt_estrutura as t4
	on t1.CD_FINAL_PARA = t4.CD_FINAL
where t1.NR_TRIMESTRE = 2  and t1.NR_ANO = 2017
and t4.NR_VERSAO = 1 and t4.NR_ANO = 2017 and t4.NR_TRIMESTRE = 3
group by CD_AGEN, 
	CAT_PROD, CD_SEGM, TIPO_REDE, TP_PESS;
quit;

proc sql;
create table joseluiz.tb_aux_meta_prod_tot_ag_gr as
select CD_AGEN, CAT_PROD, case when CD_SEGM = '6' then '5' else CD_SEGM END as CD_SEGM_AJUSTADO,
	TIPO_REDE, SUM(META_PROD_M1) AS META_PROD_GR_M1, SUM(META_PROD_M2) AS META_PROD_GR_M2
FROM joseluiz.tb_aux_meta_prod_gr
GROUP BY CD_AGEN, CAT_PROD, CD_SEGM_AJUSTADO, TIPO_REDE;
QUIT;

/* SEPARANDO META EXCLUSIVAMENTE DA AGENCIA... */
proc sql;
create table joseluiz.tb_aux_meta_prod_ag_exGR as
SELECT T1.CD_AGEN, T1.CAT_PROD, T1.CD_SEGM, T1.TIPO_REDE, T1.TP_PESS,
	CASE WHEN T1.CD_SEGM = '3' OR T1.META_PROD_M1 < T2.META_PROD_GR_M1 THEN 0
		 ELSE T1.META_PROD_M1 - T2.META_PROD_GR_M1 END AS META_PROD_AG_M1,
	CASE WHEN T1.CD_SEGM = '3' OR T1.META_PROD_M2 < T2.META_PROD_GR_M2 THEN 0
		 ELSE T1.META_PROD_M2 - T2.META_PROD_GR_M2 END AS META_PROD_AG_M2
FROM joseluiz.tb_aux_meta_prod_ag AS T1
INNER JOIN joseluiz.tb_aux_meta_prod_tot_ag_gr AS T2
ON T1.CD_AGEN = T2.CD_AGEN
	AND T1.CAT_PROD = T2.CAT_PROD
	AND T1.CD_SEGM = T2.CD_SEGM_AJUSTADO
	AND T1.TIPO_REDE = T2.TIPO_REDE;
QUIT;

GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;


/*   START OF NODE: Junta SM e Meta PROD e Accrual AG e GR   */
%LET _CLIENTTASKLABEL='Junta SM e Meta PROD e Accrual AG e GR';
%LET _CLIENTPROJECTPATH='C:\Users\T694592\Desktop\working_sas\Metas_Saldos_GR_v2.egp';
%LET _CLIENTPROJECTNAME='Metas_Saldos_GR_v2.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;


/* Primeiro passo para c·lculo da Meta de Saldo : Juntando saldo mÈdio, baixas (perda) histÛrica e meta de produÁ„o 
na mesama base e por segmento...!) */


data joseluiz.tb_aux_saldos_gr;
set JOSELUIZ.TB_SALDOS_ATIVOS_GR_HIST_REDE (where=(CD_PERI = 201703)); /* Para fins de Back teste: CD_PERI=201701*/
run;

proc sort data=joseluiz.tb_aux_saldos_gr(keep=CAT_PROD CD_AGEN CD_MATR TP_PESS CD_SEGM TIPO_REDE VL_SALD_MEDI_M0);
	by CAT_PROD CD_AGEN CD_MATR TP_PESS CD_SEGM;
run;

proc sort data=joseluiz.tb_aux_calcula_baixas_ativos_gr (keep= CAT_PROD CD_AGEN CD_MATR TP_PESS CD_SEGM AUX_PERDA2);
	by CAT_PROD CD_AGEN CD_MATR TP_PESS CD_SEGM;
run;

proc sort data=joseluiz.tb_aux_meta_prod_gr (keep= CAT_PROD CD_AGEN CD_MATR TP_PESS CD_SEGM META_PROD_M1 META_PROD_M2);
	by CAT_PROD CD_AGEN CD_MATR TP_PESS CD_SEGM;
run;


data    JOSELUIZ.tb_aux_sald_baixas_meta_prod_gr;
	merge JOSELUIZ.tb_aux_saldos_gr (IN=A)
		  joseluiz.tb_aux_calcula_baixas_ativos_gr (IN=B)
		  joseluiz.tb_aux_meta_prod_gr (IN=C);
		  /* Para fins de Back Test:  joseluiz.tb_aux_bktest_meta_prod_gr (IN=C);*/
	by CAT_PROD CD_AGEN CD_MATR TP_PESS CD_SEGM;
	if A;
	rename VL_SALD_MEDI_M0=VL_SALD_MEDI_PARTIDA AUX_PERDA2=FT_BAIXA_ACCRUAL;
run;

/* Base de elegÌveis.. necess·ria para separar GRs ElegÌveis do restante da agÍncia.....*/
data joseluiz.tb_aux_gr_ativ;
set incent.TB_SDM_BASE_GR (where=(NR_ANO=2017 and NR_TRIMESTRE = 2 and NR_MES = 1 and STATUS = 'ATIVO') /* back test: NR_TRIMESTRE = 1 and NR_MES = 1 */
 keep= MATRICULA STATUS NR_ANO NR_TRIMESTRE NR_MES);
drop STATUS NR_ANO NR_TRIMESTRE NR_MES;
run;


proc sort data=joseluiz.tb_aux_gr_ativ ;
	by MATRICULA;
run;

proc sort data=joseluiz.tb_aux_sald_baixas_meta_prod_gr;
	by CD_MATR;
run;


data    joseluiz.tb_aux_calcula_meta_ativos_gr;
	merge JOSELUIZ.tb_aux_gr_ativ (IN=A rename=MATRICULA=CD_MATR)
		  joseluiz.tb_aux_sald_baixas_meta_prod_gr (IN=B);
	by CD_MATR;
	if A;
run;


data    joseluiz.tb_aux_calcula_meta_ativos_gg;
	merge JOSELUIZ.tb_aux_gr_ativ (IN=A rename=MATRICULA=CD_MATR)
		  joseluiz.tb_aux_sald_baixas_meta_prod_gr (IN=B);
	by CD_MATR;
	if not A;
run;


PROC SQL;
CREATE TABLE joseluiz.tb_aux_sm_acrual_ativos_ag AS
SELECT CAT_PROD, CD_SEGM, CD_AGEN, TP_PESS, TIPO_REDE,
sum(VL_SALD_MEDI_PARTIDA) as VL_SALD_MEDI_PARTIDA,
avg(FT_BAIXA_ACCRUAL) as FT_BAIXA_ACCRUAL
from joseluiz.tb_aux_calcula_meta_ativos_gg
group by CAT_PROD, CD_SEGM, CD_AGEN, TP_PESS, TIPO_REDE;
quit;


proc sql;
create table joseluiz.tb_aux_calcula_meta_ativos_ag as
select  t1.CAT_PROD, t1.CD_SEGM, t1.TIPO_REDE,
t1.CD_AGEN, 999999 as CD_MATR /* GG FAKE */, t1.TP_PESS,
t1.VL_SALD_MEDI_PARTIDA,
t1.FT_BAIXA_ACCRUAL, t2.META_PROD_AG_M1 as META_PROD_M1, t2.META_PROD_AG_M2 as META_PROD_M2
from joseluiz.tb_aux_sm_acrual_ativos_ag as t1
	left join joseluiz.tb_aux_meta_prod_ag_exGR as t2
	on t1.CD_AGEN = t2.CD_AGEN
		and t1.CAT_PROD = t2.CAT_PROD
		and t1.TP_PESS = t2.TP_PESS
		and t1.CD_SEGM = t2.CD_SEGM;
quit;


data    JOSELUIZ.tb_aux_calcula_meta_ativos; 
        set JOSELUIZ.tb_aux_calcula_meta_ativos_gr 
                JOSELUIZ.tb_aux_calcula_meta_ativos_ag; 
		TIPO_PESSOA = "ND";
		if TP_PESS = 1 then TIPO_PESSOA="PF";
		if TP_PESS = 2 then TIPO_PESSOA="PJ";
run;



GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;


/*   START OF NODE: Aux Back Test GR   */
%LET _CLIENTTASKLABEL='Aux Back Test GR';
%LET _CLIENTPROJECTPATH='C:\Users\T694592\Desktop\working_sas\Metas_Saldos_GR_v2.egp';
%LET _CLIENTPROJECTNAME='Metas_Saldos_GR_v2.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;


/* Para fins de back test 
LIBNAME ANA "/user/user_portifolio_pf2/ANA/"; 
ANA.CRES_RESULTADO3
*/
proc sql;
create table joseluiz.tb_aux_bktest_meta_prod_gr as
select t1.DT_ANOMES as CD_PERI, t3.MATRICULA as CD_MATR, t4.UNIORG as CD_AGEN, 
		t2.BLOCO_SALDO as CAT_PROD, t3.CD_SEGM, t3.TIPO_REDE, t3.TP_PESS,
		sum(t1.VL_META*1000) as META_BKTEST_PROD,
		sum(t1.VL_REALIZADO*1000) as REAL_PROD_BKTEST
	/*	sum(case when t1.DT_ANOMES = 201701 then t1.VL_META*1000 else 0 end) as META_BKTEST_PROD_M0, 
		sum(case when t1.DT_ANOMES = 201702 then t1.VL_META*1000 else 0 end) as META_BKTEST_PROD_M1, 
		sum(case when t1.DT_ANOMES = 201703 then t1.VL_META*1000 else 0 end) as META_BKTEST_PROD_M2,
		sum(case when t1.DT_ANOMES = 201701 then t1.VL_REALIZADO*1000 else 0 end) as REAL_PROD_BKTEST_M0,
		sum(case when t1.DT_ANOMES = 201702 then t1.VL_REALIZADO*1000 else 0 end) as REAL_PROD_BKTEST_M1, 
		sum(case when t1.DT_ANOMES = 201703 then t1.VL_REALIZADO*1000 else 0 end) as REAL_PROD_BKTEST_M2*/
from incent.TB_MG_EXPORT00 as t1 
	inner join (SELECT DISTINCT BLOCO_SALDO, PRODUTO_GR
FROM joseluiz.TB_JL_DE_PARA_PROD_SALDOS) as t2
	on t1.CD_SRK = t2.PRODUTO_GR
	inner join joseluiz.tb_aux_de_para_categ_gr_segm_pnl as t3
	on t1.CD_UNID = t3.MATRICULA_TEXTO
	inner join incent.tb_mt_estrutura as t4
	on t1.CD_PTVD = t4.CD_FINAL
where t1.DT_ANOMES in (201701,201702, 201703)
and t4.NR_VERSAO = 1 and t4.NR_ANO = 2017 and t4.NR_TRIMESTRE = 1
group by t1.DT_ANOMES, t3.MATRICULA, t4.UNIORG, 
	t2.BLOCO_SALDO, t3.CD_SEGM, t3.TIPO_REDE, t3.TP_PESS;
quit;


proc sort data=joseluiz.tb_aux_bktest_meta_prod_gr;
	by CD_PERI CAT_PROD CD_AGEN CD_MATR TP_PESS CD_SEGM;
run;

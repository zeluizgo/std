/*   START OF NODE: HistÛrico de Saldo MÈdio de Passivos do Painel   */
%LET _CLIENTTASKLABEL='HistÛrico de Saldo MÈdio de Passivos do Painel';
%LET _CLIENTPROJECTPATH='C:\Users\T694592\Desktop\working_sas\Metas_Saldos_GR_v2.egp';
%LET _CLIENTPROJECTNAME='Metas_Saldos_GR_v2.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;
/*LIBNAME KODAMA  '/user/user_info/KODAMA';*/
LIBNAME CADASTRO ORACLE USER = INCENTIVOS PW = '**********' PATH = 'ORAPR072' SCHEMA = CADASTRO;

/* Base de saldos somente rede Padr„o!! */
PROC SQL;
   CREATE TABLE JOSELUIZ.TB_SALDOS_PASSIV_GR_HIST_PADRAO AS 
   SELECT t1.CD_PERI, 
   		  t1.TP_PESS,
          t1.CD_SEGM, 
          t1.CD_AGEN_VF as CD_AGEN, 
          t1.CD_MATR_VF as CD_MATR, 
          t1.CAT_PROD,
		  case when t1.CD_PVEN_VF eq . then t1.CD_AGEN_VF else t1.CD_PVEN_VF end as CD_PVEN_VF2,
		  "PADRAO" AS TIPO_REDE,
            (SUM(t1.VL_SALD_MEDI_M0_PASS))     AS VL_SALD_MEDI_M0,
                (SUM(t1.VL_SALD_MEDI_M1_PASS)) AS VL_SALD_MEDI_M1,
                (SUM(t1.VL_NOVA_APLC))              AS VL_ENTRADA_NOVA,
                (SUM(t1.VL_NOVO_APRT))              AS VL_ENTRADA_EXIS,
                (SUM(t1.VL_RESG_PARC))               AS VL_SAIDA_PARC,
                (SUM(t1.VL_RESG_TOTL))               AS VL_SAIDA_TOT,
                (SUM(t1.GANHO_MATR_VF_MIGR))   AS GANHO_MATR_MIGR,
                (SUM(t1.PERDA_MATR_VF_MIGR))   AS PERDA_MATR_MIGR,
                (SUM(t1.DELT_SALD_MEDI_PASS)) AS DELT_SALD_MEDI

      FROM CADASTRO.TB_PNEL_CERT_SALDOS_SUM_PAS_PR t1
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
   CREATE TABLE JOSELUIZ.TB_SALDOS_PASSIV_GR_HIST_REMOTO AS 
   SELECT t1.CD_PERI, 
   		  t1.TP_PESS,
          t1.CD_SEGM, 
          t1.CD_AGEN_REMO as CD_AGEN,  
          t1.CD_MATR_REMO as CD_MATR, 
          t1.CAT_PROD, 
         /* t1.CD_PVEN_VF, */
		  "REMOTO" AS TIPO_REDE,
            (SUM(t1.VL_SALD_MEDI_M0_PASS))     AS VL_SALD_MEDI_M0,
                (SUM(t1.VL_SALD_MEDI_M1_PASS)) AS VL_SALD_MEDI_M1,
                (SUM(t1.VL_NOVA_APLC))              AS VL_ENTRADA_NOVA,
                (SUM(t1.VL_NOVO_APRT))              AS VL_ENTRADA_EXIS,
                (SUM(t1.VL_RESG_PARC))               AS VL_SAIDA_PARC,
                (SUM(t1.VL_RESG_TOTL))               AS VL_SAIDA_TOT,
                (SUM(t1.GANHO_MATR_REMO_MIGR))   AS GANHO_MATR_MIGR,
                (SUM(t1.PERDA_MATR_REMO_MIGR))   AS PERDA_MATR_MIGR,
                (SUM(t1.DELT_SALD_MEDI_PASS)) AS DELT_SALD_MEDI


      FROM CADASTRO.TB_PNEL_CERT_SALDOS_SUM_PAS_PR  t1
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

proc sort data=JOSELUIZ.TB_SALDOS_PASSIV_GR_HIST_PADRAO ;
	by CD_AGEN CD_PVEN_VF2;
run;

proc sort data=joseluiz.tb_aux_de_para_cdniorg_ag_altair;
	by CD_AGEN CD_PVEN_VF;
run;


data    JOSELUIZ.TB_SALDOS_PASSIV_GR_HIST_PADRAO;
	merge JOSELUIZ.TB_SALDOS_PASSIV_GR_HIST_PADRAO (IN=A rename=CD_PVEN_VF2=CD_PVEN_VF)
		  joseluiz.tb_aux_de_para_cdniorg_ag_altair (IN=B);
	by CD_AGEN CD_PVEN_VF;
	if A and B;
run;


/* Ajuste MatrÌcula Carteira Compartilhada */
data JOSELUIZ.TB_SALDOS_PASSIV_GR_HIST_PADRAO;
set JOSELUIZ.TB_SALDOS_PASSIV_GR_HIST_PADRAO;
if CD_MATR = 7251403 and UNIORG < 20000 then CD_MATR = 99990000000 + UNIORG;
if CD_MATR = 7251403 and UNIORG > 20000 then CD_MATR = 99980000000 + UNIORG;
run;

proc sort data=JOSELUIZ.TB_SALDOS_PASSIV_GR_HIST_REMOTO ;
	by CD_AGEN ;
run;

proc sort data=joseluiz.tb_aux_de_para_altair;
	by CD_AGEN ;
run;


data    JOSELUIZ.TB_SALDOS_PASSIV_GR_HIST_REMOTO;
	merge JOSELUIZ.TB_SALDOS_PASSIV_GR_HIST_REMOTO (IN=A)
		  joseluiz.tb_aux_de_para_altair (IN=B);
	by CD_AGEN ;
	if A and B;
run;



/* Base de Saldos Total da Rede */
data    JOSELUIZ.TB_SALDOS_PASSIV_GR_HIST_REDE; 
        set JOSELUIZ.TB_SALDOS_PASSIV_GR_HIST_PADRAO 
                JOSELUIZ.TB_SALDOS_PASSIV_GR_HIST_REMOTO; 
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


/*   START OF NODE: Junta SM e Meta PROD AG e GR   */
%LET _CLIENTTASKLABEL='Junta SM e Meta PROD AG e GR';
%LET _CLIENTPROJECTPATH='C:\Users\T694592\Desktop\working_sas\Metas_Saldos_GR_v2.egp';
%LET _CLIENTPROJECTNAME='Metas_Saldos_GR_v2.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;


/* Primeiro passo para c·lculo da Meta de Saldo : Juntando saldo mÈdio, baixas (perda) histÛrica e meta de produÁ„o 
na mesama base e por segmento...!) */


data joseluiz.tb_aux_sald_passiv_gr;
set JOSELUIZ.TB_SALDOS_PASSIV_GR_HIST_REDE (where=(CD_PERI = 201703)); /* Para fins de Back teste: CD_PERI=201701*/
run;

proc sort data=joseluiz.tb_aux_sald_passiv_gr(keep=CAT_PROD CD_AGEN CD_MATR TP_PESS CD_SEGM TIPO_REDE VL_SALD_MEDI_M0);
	by CAT_PROD CD_AGEN CD_MATR TP_PESS CD_SEGM;
run;

proc sort data=joseluiz.tb_aux_meta_prod_gr (keep= CAT_PROD CD_AGEN CD_MATR TP_PESS CD_SEGM META_PROD_M1 META_PROD_M2);
	by CAT_PROD CD_AGEN CD_MATR TP_PESS CD_SEGM;
run;

data    JOSELUIZ.tb_aux_sald_passiv_meta_prod_gr;
	merge JOSELUIZ.tb_aux_sald_passiv_gr (IN=A)
		 joseluiz.tb_aux_meta_prod_gr (IN=B);
		   /* Para fins de Back Test:  joseluiz.tb_aux_bktest_meta_prod_gr (IN=B);*/
	by CAT_PROD CD_AGEN CD_MATR TP_PESS CD_SEGM;
	if A and B; ;/* Aqui Rodrigo 1: Trazer Meta de ProduÁ„o para quem n„o veio! */
	rename VL_SALD_MEDI_M0=VL_SALD_MEDI_PARTIDA;
run;

data    JOSELUIZ.tb_aux_somente_meta_prod_gr;
	merge JOSELUIZ.tb_aux_sald_passiv_gr (IN=A)
		 joseluiz.tb_aux_meta_prod_gr (IN=B);
		   /* Para fins de Back Test:  joseluiz.tb_aux_bktest_meta_prod_gr (IN=B);*/
	by CAT_PROD CD_AGEN CD_MATR TP_PESS CD_SEGM;
	if not A; ;/* Aqui Rodrigo 1: Trazer Meta de ProduÁ„o para quem n„o veio! */
	rename VL_SALD_MEDI_M0=VL_SALD_MEDI_PARTIDA;
run;


data    JOSELUIZ.tb_aux_somente_sald_passiv_gr;
	merge JOSELUIZ.tb_aux_sald_passiv_gr (IN=A)
		 joseluiz.tb_aux_meta_prod_gr (IN=B);
		   /* Para fins de Back Test:  joseluiz.tb_aux_bktest_meta_prod_gr (IN=B);*/
	by CAT_PROD CD_AGEN CD_MATR TP_PESS CD_SEGM;
	if not B; ;/* Aqui Rodrigo 1: Trazer Meta de ProduÁ„o para quem n„o veio! */
	rename VL_SALD_MEDI_M0=VL_SALD_MEDI_PARTIDA;
	CD_MATR = "999999";
run;

data    JOSELUIZ.tb_aux_sald_passiv_meta_prod_gr;
set JOSELUIZ.tb_aux_sald_passiv_meta_prod_gr
	JOSELUIZ.tb_aux_somente_meta_prod_gr
	JOSELUIZ.tb_aux_somente_sald_passiv_gr;
run;


proc sort data=joseluiz.tb_aux_sald_passiv_meta_prod_gr;
	by CAT_PROD;
run;

proc sort data=joseluiz.TB_JL_PARAM_MT_SLD_INVEST_FATOR (keep= GRUPO_PROD FATOR_PERDA);
	by GRUPO_PROD;
run;


/* TODO: FT_BAIXA_ACCRUAL (Importante Parametrizar por Segmento) */
data    JOSELUIZ.tb_aux_sald_passiv_meta_prod_gr;
	merge JOSELUIZ.tb_aux_sald_passiv_meta_prod_gr (IN=A)
		  joseluiz.TB_JL_PARAM_MT_SLD_INVEST_FATOR (IN=B rename=GRUPO_PROD=CAT_PROD);
	by CAT_PROD;
	if A and B;
	rename FATOR_PERDA=FT_BAIXA_ACCRUAL;
run;



/* Base de elegÌveis.. necess·ria para separar GRs ElegÌveis do restante da agÍncia.....*/
data joseluiz.tb_aux_gr_ativ;
set incent.TB_SDM_BASE_GR (where=(NR_ANO=2017 and NR_TRIMESTRE = 2 and NR_MES = 1 and STATUS = 'ATIVO') /* back test:NR_TRIMESTRE = 1 and  NR_MES = 1 */
 keep= MATRICULA STATUS NR_ANO NR_TRIMESTRE NR_MES);
drop STATUS NR_ANO NR_TRIMESTRE NR_MES;
run;


proc sort data=joseluiz.tb_aux_gr_ativ ;
	by MATRICULA;
run;

proc sort data=joseluiz.tb_aux_sald_passiv_meta_prod_gr;
	by CD_MATR;
run;


data    joseluiz.tb_aux_calcula_meta_PASSIV_gr;
	merge JOSELUIZ.tb_aux_gr_ativ (IN=A rename=MATRICULA=CD_MATR)
		  joseluiz.tb_aux_sald_passiv_meta_prod_gr (IN=B);
	by CD_MATR;
	if A;
run;


data    joseluiz.tb_aux_calcula_meta_PASSIV_gg;
	merge JOSELUIZ.tb_aux_gr_ativ (IN=A rename=MATRICULA=CD_MATR)
		  joseluiz.tb_aux_sald_passiv_meta_prod_gr (IN=B);
	by CD_MATR;
	if not A;
run;


PROC SQL;
CREATE TABLE joseluiz.tb_aux_sm_acrual_PASSIV_ag AS
SELECT CAT_PROD, CD_SEGM, CD_AGEN, TP_PESS, TIPO_REDE,
sum(VL_SALD_MEDI_PARTIDA) as VL_SALD_MEDI_PARTIDA,
avg(FT_BAIXA_ACCRUAL) as FT_BAIXA_ACCRUAL
from joseluiz.tb_aux_calcula_meta_PASSIV_gg
group by CAT_PROD, CD_SEGM, CD_AGEN, TP_PESS, TIPO_REDE;
quit;


proc sql;
create table joseluiz.tb_aux_calcula_meta_PASSIV_ag as
select  t1.CAT_PROD, t1.CD_SEGM, t1.TIPO_REDE,
t1.CD_AGEN, 999999 as CD_MATR /* GG FAKE */, t1.TP_PESS,
t1.VL_SALD_MEDI_PARTIDA,
t1.FT_BAIXA_ACCRUAL, t2.META_PROD_AG_M1 as META_PROD_M1, t2.META_PROD_AG_M2 as META_PROD_M2
from joseluiz.tb_aux_sm_acrual_PASSIV_ag as t1
	left join joseluiz.tb_aux_meta_prod_ag_exGR as t2
	on t1.CD_AGEN = t2.CD_AGEN
		and t1.CAT_PROD = t2.CAT_PROD
		and t1.TP_PESS = t2.TP_PESS
		and t1.CD_SEGM = t2.CD_SEGM;
quit;


data    JOSELUIZ.tb_aux_calcula_meta_PASSIV; 
        set JOSELUIZ.tb_aux_calcula_meta_PASSIV_gr 
                JOSELUIZ.tb_aux_calcula_meta_PASSIV_ag; 
		TIPO_PESSOA = "ND";
		if TP_PESS = 1 then TIPO_PESSOA="PF";
		if TP_PESS = 2 then TIPO_PESSOA="PJ";
run;

GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;


/*   START OF NODE: Calcula Meta Saldo Medio ALL   */
%LET _CLIENTTASKLABEL='Calcula Meta Saldo Medio ALL';
%LET _CLIENTPROJECTPATH='C:\Users\T694592\Desktop\working_sas\Metas_Saldos_GR_v3.egp';
%LET _CLIENTPROJECTNAME='Metas_Saldos_GR_v3.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;
data    JOSELUIZ.tb_aux_calcula_meta_ativos; 
set    JOSELUIZ.tb_aux_calcula_meta_ativos; 
META_BETA_M1 = sum(VL_SALD_MEDI_PARTIDA * FT_BAIXA_ACCRUAL , META_PROD_M1/2); 
META_BETA_M2 = sum(sum(VL_SALD_MEDI_PARTIDA,META_PROD_M1) * FT_BAIXA_ACCRUAL , META_PROD_M1/2 , META_PROD_M2/2); 

if TIPO_REDE="REMOTO" then
do;
	FT_BAIXA_ACCRUAL = -0.002;
	META_BETA_M1 = sum(VL_SALD_MEDI_PARTIDA * FT_BAIXA_ACCRUAL , META_PROD_M1/3); 
	META_BETA_M2 = sum(sum(VL_SALD_MEDI_PARTIDA,META_PROD_M1) * FT_BAIXA_ACCRUAL , META_PROD_M1/3 , META_PROD_M2/3);
end;
run;

data    JOSELUIZ.tb_aux_calcula_meta_PASSIV; 
set    JOSELUIZ.tb_aux_calcula_meta_PASSIV; 
if TIPO_PESSOA = "PJ" then 
do;
	META_BETA_M1 = sum(VL_SALD_MEDI_PARTIDA * FT_BAIXA_ACCRUAL , META_PROD_M1); /* Aqui Rodrigo 2: Incluir impato do mÍs anterior dado que o saldo mÈdio de maprtida e M-1 (falta M0) Meta È para M1 e M2 ! */ 
	META_BETA_M2 = sum(sum(VL_SALD_MEDI_PARTIDA,META_PROD_M1) * FT_BAIXA_ACCRUAL , META_PROD_M1/2 , META_PROD_M2/2);
end; 
if TIPO_PESSOA = "PF" then 
do;
	/* TODO: FT_BAIXA_ACCRUAL (Importante Parametrizar por Segmento) */
	if CD_SEGM eq '1' then
	do;
		FT_BAIXA_ACCRUAL = 0.009;
		META_BETA_M1 = sum(VL_SALD_MEDI_PARTIDA * FT_BAIXA_ACCRUAL , META_PROD_M1/2); /* Aqui na PF tentando adequar ‡ global: reduzindo %de cresc. vegetativa da carteira e impacto da cap liq no saldo mÈdio (maior parte da produÁ„o no final do mÍs)*/ 
		META_BETA_M2 = sum(sum(VL_SALD_MEDI_PARTIDA,META_PROD_M1) * FT_BAIXA_ACCRUAL , META_PROD_M1/2 , META_PROD_M2/2);
	end;

	if CD_SEGM ne '1' then
	do;
		FT_BAIXA_ACCRUAL = 0.003;
		META_BETA_M1 = sum(VL_SALD_MEDI_PARTIDA * FT_BAIXA_ACCRUAL , META_PROD_M1/2.8); /* Aqui na PF tentando adequar ‡ global: reduzindo %de cresc. vegetativa da carteira e impacto da cap liq no saldo mÈdio (maior parte da produÁ„o no final do mÍs)*/ 
		META_BETA_M2 = sum(sum(VL_SALD_MEDI_PARTIDA,META_PROD_M1) * FT_BAIXA_ACCRUAL , META_PROD_M1/3.5 , META_PROD_M2/3.5);
	end;
end; 
run;

data    JOSELUIZ.tb_aux_pondera_meta_all; 
        set JOSELUIZ.tb_aux_calcula_meta_PASSIV(/*where=(TIPO_REDE="PADRAO")*/) 
                JOSELUIZ.tb_aux_calcula_meta_ativos(/*where=(TIPO_REDE="PADRAO")*/); 
run;

proc sort data=joseluiz.tb_aux_pondera_meta_all ;
	by CAT_PROD;
run;

proc sort data=joseluiz.TB_JL_DE_PARA_BLOCO_SALDOS;
	by BLOCO_SALDO;
run;

/* Agrupando produtos nos blocos de Metas : Cres. Emprestimos Foco e n„o Foco (EX_FOCO) e Cresc de Investimentos. */
data    joseluiz.tb_aux_pondera_meta_all;
	merge JOSELUIZ.tb_aux_pondera_meta_all (IN=A)
		  joseluiz.TB_JL_DE_PARA_BLOCO_SALDOS (IN=B  rename=BLOCO_SALDO=CAT_PROD);
	by CAT_PROD;
	if A;
run;

proc sql;

create table joseluiz.tb_aux_pondera_meta_gr_all as
select CD_MATR, CD_AGEN, TIPO_PESSOA, AGRUPA_SALDO,TIPO_REDE, 
	sum(VL_SALD_MEDI_PARTIDA) as SALDO,
	sum(META_PROD_M1) as META_PROD_M1,
	sum(META_PROD_M2) as META_PROD_M2, /* Para fins de BAck Test:...
	sum(REAL_PROD_BKTEST_M1) as REAL_PROD_M1,
	sum(REAL_PROD_BKTEST_M2) as REAL_PROD_M2,*/
	sum(META_BETA_M1) as META_BETA_M1,
	sum(META_BETA_M2) as META_BETA_M2
from joseluiz.tb_aux_pondera_meta_all
group by CD_MATR, CD_AGEN, TIPO_PESSOA, AGRUPA_SALDO, TIPO_REDE
order by TIPO_PESSOA, AGRUPA_SALDO, CD_AGEN;

quit;

data joseluiz.tb_aux_total_ag_pondera_meta;
set joseluiz.tb_aux_pondera_meta_gr_all;
by TIPO_PESSOA AGRUPA_SALDO CD_AGEN;
retain SALDO_AG META_PROD_M1_AG META_PROD_M2_AG META_BETA_M1_AG META_BETA_M2_AG;
if first.CD_AGEN then
do;
	SALDO_AG = 0;
	META_PROD_M1_AG = 0;
	META_PROD_M2_AG = 0;
	META_BETA_M1_AG = 0;
	META_BETA_M2_AG = 0;
end;
SALDO_AG = sum(SALDO_AG,SALDO);
META_PROD_M1_AG = sum(META_PROD_M1_AG,META_PROD_M1);
META_PROD_M2_AG = sum(META_PROD_M2_AG,META_PROD_M2);
META_BETA_M1_AG = sum(META_BETA_M1_AG,META_BETA_M1);
META_BETA_M2_AG = sum(META_BETA_M2_AG,META_BETA_M2);
if last.CD_AGEN then output;
drop CD_MATR SALDO META_PROD_M1 META_PROD_M2 META_BETA_M1 META_BETA_M2;
run; 


/* Agrupando produtos nos blocos de Metas : Cres. Emprestimos Foco e n„o Foco (EX_FOCO) e Cresc de Investimentos. */
data    joseluiz.tb_aux_pondera_meta_gr_all;
	merge joseluiz.tb_aux_pondera_meta_gr_all (IN=A)
		  joseluiz.tb_aux_total_ag_pondera_meta (IN=B);
	by TIPO_PESSOA AGRUPA_SALDO CD_AGEN;
	if A;

	if SALDO_AG ne 0 then PART_GR_SALDO = SALDO/SALDO_AG;
	if META_PROD_M1_AG ne 0 then PART_GR_META_PROD_M1 = META_PROD_M1/META_PROD_M1_AG;
	if META_PROD_M2_AG ne 0 then PART_GR_META_PROD_M2 = META_PROD_M2/META_PROD_M2_AG;
	if META_BETA_M1_AG ne 0 then PART_GR_META_BETA_M1 = META_BETA_M1/META_BETA_M1_AG;
	if META_BETA_M2_AG ne 0 then PART_GR_META_BETA_M2 = META_BETA_M2/META_BETA_M2_AG;
run;


proc sort data=joseluiz.tb_aux_pondera_meta_gr_all ;
	by CD_AGEN TIPO_PESSOA;
run;

proc sort data=joseluiz.TB_JL_META_CRESC_EMPR_3BI17;
	by UNIORG TIPO_PESSOA;
run;

proc sort data=joseluiz.TB_JL_META_CRESC_INVEST_3BI17;
	by UNIORG TIPO_PESSOA;
run;


data    joseluiz.tb_aux_pondera_meta_gr_all;
	merge joseluiz.tb_aux_pondera_meta_gr_all (IN=A)
		  joseluiz.TB_JL_META_CRESC_EMPR_3BI17 (IN=B  rename=UNIORG=CD_AGEN)
		  joseluiz.TB_JL_META_CRESC_INVEST_3BI17 (IN=C  rename=UNIORG=CD_AGEN);
	by CD_AGEN TIPO_PESSOA;
	if A;
	META_SALDO_M1_AG = 0;
	META_SALDO_M2_AG = 0;
	if AGRUPA_SALDO = "CRESC_INVEST" then
		do;
			META_SALDO_M1_AG = CRESC_INVEST_M1;
			META_SALDO_M2_AG = CRESC_INVEST_M2;
		end;
	if AGRUPA_SALDO = "CRESC_EMPR_FOCO" then
		do;
			META_SALDO_M1_AG = CRESC_EMPR_FOCO_M1;
			META_SALDO_M2_AG = CRESC_EMPR_FOCO_M2;
		end;
		/* Mantendo mnemonico EX_FOCO porÈm refindo-se ao TOTAL....*/
	if AGRUPA_SALDO = "CRESC_EMPR_EX_FOCO" then
		do;
			META_SALDO_M1_AG = CRESC_EMPR_TOT_M1-CRESC_EMPR_FOCO_M1;
			META_SALDO_M2_AG = CRESC_EMPR_TOT_M2-CRESC_EMPR_FOCO_M2;
		end;

drop CRESC_EMPR_TOT_M1   CRESC_EMPR_TOT_M2	CRESC_EMPR_FOCO_M1	CRESC_EMPR_FOCO_M2 CRESC_INVEST_M1	CRESC_INVEST_M2;
run;

data joseluiz.tb_aux_pondera_meta_gr_foco;
set  joseluiz.tb_aux_pondera_meta_gr_all (where=(AGRUPA_SALDO = "CRESC_EMPR_FOCO"));
AGRUPA_SALDO = "CRESC_EMPR_EX_FOCO"; /* Para dar match no merge !! */
/*drop META_PROD_M1	META_PROD_M2	META_PROD_M1_AG	META_PROD_M2_AG	PART_GR_META_PROD_M1	PART_GR_META_PROD_M2;*/
run;

proc sort data=joseluiz.tb_aux_pondera_meta_gr_all;
	by CD_MATR	CD_AGEN	 TIPO_PESSOA	AGRUPA_SALDO;
run;

proc sort data=joseluiz.tb_aux_pondera_meta_gr_foco;
	by CD_MATR	CD_AGEN	 TIPO_PESSOA	AGRUPA_SALDO;
run;

data    joseluiz.tb_aux_pondera_meta_gr_all;
	merge joseluiz.tb_aux_pondera_meta_gr_all (IN=A)
		  joseluiz.tb_aux_pondera_meta_gr_foco (IN=B  rename=(SALDO=SALDO_FOCO SALDO_AG=SALDO_AG_FOCO META_SALDO_M1_AG=META_SALDO_M1_AG_FOCO META_SALDO_M2_AG=META_SALDO_M2_AG_FOCO));
	by CD_MATR	CD_AGEN	 TIPO_PESSOA	AGRUPA_SALDO;
	if A;

	if sum(SALDO_AG,SALDO_AG_FOCO) ne 0 then PART_GR_SALDO = sum(SALDO,SALDO_FOCO)/sum(SALDO_AG,SALDO_AG_FOCO);
	SALDO=sum(SALDO,SALDO_FOCO);
	SALDO_AG=sum(SALDO_AG,SALDO_AG_FOCO);
	META_SALDO_M1_AG=sum(META_SALDO_M1_AG,META_SALDO_M1_AG_FOCO);
	META_SALDO_M2_AG=sum(META_SALDO_M2_AG,META_SALDO_M2_AG_FOCO);

	drop SALDO_AG_FOCO SALDO_FOCO META_SALDO_M1_AG_FOCO META_SALDO_M2_AG_FOCO;

run;


proc sort data=joseluiz.TB_JL_UNIDADES_POLOS ;
	by UNIORG_POLO;
run;

proc sort data=joseluiz.tb_aux_pondera_meta_gr_all;
	by CD_AGEN;
run;


data    joseluiz.tb_aux_pondera_meta_gr_all;
	merge joseluiz.tb_aux_pondera_meta_gr_all (IN=A)
		  joseluiz.TB_JL_UNIDADES_POLOS (IN=B rename=UNIORG_POLO=CD_AGEN);
	by CD_AGEN;
	if A;
	FLAG_POLO = 0;
	if B then FLAG_POLO = 1;
run;


proc sort data=joseluiz.tb_aux_de_para_categ_gr_segm_pnl ;
	by MATRICULA;
run;

proc sort data=joseluiz.tb_aux_pondera_meta_gr_all;
	by CD_MATR;
run;


data    joseluiz.tb_aux_pondera_meta_gr_all;
	merge joseluiz.tb_aux_pondera_meta_gr_all (IN=A)
		  joseluiz.tb_aux_de_para_categ_gr_segm_pnl (IN=B rename=MATRICULA=CD_MATR keep=MATRICULA CD_SEGM);
	by CD_MATR;
	if A;
	VISAO_NEG = "NDA";
	if CD_SEGM = '1' then VISAO_NEG = "SEL";
	if CD_SEGM = '2' then VISAO_NEG = "VG";
	if CD_SEGM = '3' then VISAO_NEG = "CC";
	if CD_SEGM = '5' then VISAO_NEG = "E1";
	if CD_SEGM = '6' then VISAO_NEG = "E2";
	drop CD_SEGM;

/* aplicando fatores para fechar com global */ 


	if TIPO_REDE="REMOTO" and TIPO_PESSOA = "PJ" and AGRUPA_SALDO = "CRESC_INVEST" then
	do;
			META_FINAL_GR_M1 = META_BETA_M1 * 1.3;
			META_FINAL_GR_M2 = META_BETA_M2 * 2.0;
	end;
	
	if TIPO_REDE="PADRAO" and TIPO_PESSOA = "PJ" and AGRUPA_SALDO = "CRESC_INVEST" then
	do;
			META_FINAL_GR_M1 = META_BETA_M1 * 0.7;
			META_FINAL_GR_M2 = META_BETA_M2 * 1.23;
	end;
	
	if TIPO_REDE="REMOTO" and TIPO_PESSOA = "PJ" and AGRUPA_SALDO ne "CRESC_INVEST" then
	do;
			META_FINAL_GR_M1 = META_BETA_M1 * 0.9;
			META_FINAL_GR_M2 = META_BETA_M2 * 0.9;
	end;
	
	if TIPO_REDE="PADRAO" and TIPO_PESSOA = "PJ" and AGRUPA_SALDO ne "CRESC_INVEST" then
	do;
		if VISAO_NEG = "E1" then
			do;			
				META_FINAL_GR_M1 = META_SALDO_M1_AG*sum(PART_GR_SALDO*0.2,PART_GR_META_PROD_M1*0.8);
				META_FINAL_GR_M2 = META_SALDO_M2_AG*sum(PART_GR_SALDO*0.2,PART_GR_META_PROD_M2*0.8);				
			end;
		if VISAO_NEG = "E2" then
			do;			
				META_FINAL_GR_M1 = META_SALDO_M1_AG*sum(PART_GR_SALDO*0.7,PART_GR_META_PROD_M1*0.3);
				META_FINAL_GR_M2 = META_SALDO_M2_AG*sum(PART_GR_SALDO*0.7,PART_GR_META_PROD_M2*0.3);			
			end;
		if VISAO_NEG = "NDA" then
			do;			
				META_FINAL_GR_M1 = META_SALDO_M1_AG*sum(PART_GR_SALDO*1,PART_GR_META_PROD_M1*0);
				META_FINAL_GR_M2 = META_SALDO_M2_AG*sum(PART_GR_SALDO*1,PART_GR_META_PROD_M2*0);			
			end;

		if AGRUPA_SALDO = "CRESC_EMPR_FOCO" then
		do;
			META_FINAL_GR_M1 = META_FINAL_GR_M1 * 1.25;
			META_FINAL_GR_M2 = META_FINAL_GR_M2 * 3.29;
		end;

		if AGRUPA_SALDO = "CRESC_EMPR_EX_FOCO" then
		do;
			if VISAO_NEG = "E1" then
				do;			
					META_FINAL_GR_M1 = META_FINAL_GR_M1 * 1.12;
					META_FINAL_GR_M2 = META_FINAL_GR_M2 * 3.22;				
				end;
			if VISAO_NEG = "E2" then
				do;			
					META_FINAL_GR_M1 = META_FINAL_GR_M1 * 1.12;
					META_FINAL_GR_M2 = META_FINAL_GR_M2 * 3.22;			
				end;
			if VISAO_NEG = "NDA" then
				do;			
					META_FINAL_GR_M1 = META_FINAL_GR_M1 * 1.12;
					META_FINAL_GR_M2 = META_FINAL_GR_M2 * 3.22;			
				end;
		end;

	end;

	if TIPO_REDE="REMOTO" and TIPO_PESSOA = "PF" and AGRUPA_SALDO = "CRESC_INVEST" then
	do;
			META_FINAL_GR_M1 = META_BETA_M1 * 0.92;
			META_FINAL_GR_M2 = META_BETA_M2 * 0.60;
	end;

	if TIPO_REDE="PADRAO" and TIPO_PESSOA = "PF" and AGRUPA_SALDO = "CRESC_INVEST" then
	do;
		if VISAO_NEG = "SEL" then
			do;		
				META_FINAL_GR_M1 = META_BETA_M1 * 1.22;
				META_FINAL_GR_M2 = META_BETA_M2 * 1.02;				
			end;
		if VISAO_NEG ne "SEL" then
			do;		
				META_FINAL_GR_M1 = META_BETA_M1 * 1.03;
				META_FINAL_GR_M2 = META_BETA_M2 * 0.60;				
			end;
	end;
	
	
	if TIPO_REDE="REMOTO" and TIPO_PESSOA = "PF" and AGRUPA_SALDO ne "CRESC_INVEST" then
	do;
			META_FINAL_GR_M1 = META_BETA_M1 * 1.07;
			META_FINAL_GR_M2 = META_BETA_M2 * 0.30;
			if AGRUPA_SALDO = "CRESC_EMPR_EX_FOCO" then 
				do; 
					AGRUPA_SALDO = "CRESC_EMPR_TOTAL"; 
				end;
	end;
	
	if TIPO_REDE="PADRAO" and TIPO_PESSOA = "PF" and AGRUPA_SALDO ne "CRESC_INVEST" then
	do;
		if VISAO_NEG = "SEL" then
			do;			
				META_FINAL_GR_M1 = META_SALDO_M1_AG*sum(PART_GR_SALDO*0.2,PART_GR_META_PROD_M1*0.8);
				META_FINAL_GR_M2 = META_SALDO_M2_AG*sum(PART_GR_SALDO*0.2,PART_GR_META_PROD_M2*0.8);				
			end;
		if VISAO_NEG ne "SEL" then
			do;			
				META_FINAL_GR_M1 = META_SALDO_M1_AG*sum(PART_GR_SALDO*0.7,PART_GR_META_PROD_M1*0.3);
				META_FINAL_GR_M2 = META_SALDO_M2_AG*sum(PART_GR_SALDO*0.7,PART_GR_META_PROD_M2*0.3);			
			end;
		if VISAO_NEG = "NDA" then
			do;			
				META_FINAL_GR_M1 = META_SALDO_M1_AG*sum(PART_GR_SALDO*1,PART_GR_META_PROD_M1*0);
				META_FINAL_GR_M2 = META_SALDO_M2_AG*sum(PART_GR_SALDO*1,PART_GR_META_PROD_M2*0);			
			end;

		if VISAO_NEG eq "SEL" and AGRUPA_SALDO = "CRESC_EMPR_FOCO" then
		do;
			META_FINAL_GR_M1 = META_FINAL_GR_M1 * 1.04;
			META_FINAL_GR_M2 = META_FINAL_GR_M2 * 1.06;
		end;

		if VISAO_NEG ne "SEL"  and AGRUPA_SALDO = "CRESC_EMPR_FOCO" then
		do;
			META_FINAL_GR_M1 = META_FINAL_GR_M1 * 1.59;
			META_FINAL_GR_M2 = META_FINAL_GR_M2 * 0.98;
		end;

		if VISAO_NEG eq "SEL"  and AGRUPA_SALDO = "CRESC_EMPR_EX_FOCO" then
		do;
			META_FINAL_GR_M1 = META_FINAL_GR_M1 * 1.14;
			META_FINAL_GR_M2 = META_FINAL_GR_M2 * 1.13;

			AGRUPA_SALDO = "CRESC_EMPR_TOTAL";
		end;

		if VISAO_NEG ne "SEL"  and AGRUPA_SALDO = "CRESC_EMPR_EX_FOCO" then
		do;
			META_FINAL_GR_M1 = META_FINAL_GR_M1 * 1.60;
			META_FINAL_GR_M2 = META_FINAL_GR_M2 * 1.0;

			AGRUPA_SALDO = "CRESC_EMPR_TOTAL";
		end;
/*
		if AGRUPA_SALDO = "CRESC_EMPR_EX_FOCO" then
		do;
			if VISAO_NEG = "E1" then
				do;			
					META_FINAL_GR_M1 = META_FINAL_GR_M1 * 0.47;
					META_FINAL_GR_M2 = META_FINAL_GR_M2 * 1.92;				
				end;
			if VISAO_NEG = "E2" then
				do;			
					META_FINAL_GR_M1 = META_FINAL_GR_M1 * 0.47;
					META_FINAL_GR_M2 = META_FINAL_GR_M2 * 1.92;			
				end;
			if VISAO_NEG = "NDA" then
				do;			
					META_FINAL_GR_M1 = META_FINAL_GR_M1 * 0.47;
					META_FINAL_GR_M2 = META_FINAL_GR_M2 * 1.92;			
				end;
		end;
*/
	end;


/* Aplicando Meta MÌnima e M·xima p/ Investimentos ... */	
	if VISAO_NEG = "E1" and AGRUPA_SALDO = "CRESC_INVEST" then 
		do;			
			/* Aplicando MÌnimo :*/
			META_FINAL_GR_M1 = max(META_FINAL_GR_M1, 35000);
			META_FINAL_GR_M2 = max(META_FINAL_GR_M2, 44000);
			/* Aplicando Ma¥ximo: */
			META_FINAL_GR_M1 = min(META_FINAL_GR_M1, 120000);
			META_FINAL_GR_M2 = min(META_FINAL_GR_M2, 130000);
		end;
	
	if VISAO_NEG = "E2" and AGRUPA_SALDO = "CRESC_INVEST" then 
		do;			
			/* Aplicando MÌnimo :*/
			META_FINAL_GR_M1 = max(META_FINAL_GR_M1, 60000);
			META_FINAL_GR_M2 = max(META_FINAL_GR_M2, 75000);
			/* Aplicando Ma¥ximo: */
			META_FINAL_GR_M1 = min(META_FINAL_GR_M1, 330000);
			META_FINAL_GR_M2 = min(META_FINAL_GR_M2, 350000);
		end;
	
	if VISAO_NEG = "NDA" and AGRUPA_SALDO = "CRESC_INVEST" then 
		do;
			/* Aplicando Ma¥ximo: */
			META_FINAL_GR_M1 = min(META_FINAL_GR_M1, 330000);
			META_FINAL_GR_M2 = min(META_FINAL_GR_M2, 350000);
		end;
	if VISAO_NEG = "CC" and AGRUPA_SALDO = "CRESC_INVEST" then 
		do;			
			/* Aplicando MÌnimo :*/
			META_FINAL_GR_M1 = max(META_FINAL_GR_M1, 10000);
			META_FINAL_GR_M2 = max(META_FINAL_GR_M2, 10000);
			/* Aplicando Ma¥ximo: */
			META_FINAL_GR_M1 = min(META_FINAL_GR_M1, 130000);
			META_FINAL_GR_M2 = min(META_FINAL_GR_M2, 130000);
		end;
	if VISAO_NEG = "VG" and AGRUPA_SALDO = "CRESC_INVEST" then 
		do;			
			/* Aplicando MÌnimo :*/
			META_FINAL_GR_M1 = max(META_FINAL_GR_M1, 30000);
			META_FINAL_GR_M2 = max(META_FINAL_GR_M2, 30000);
			/* Aplicando M·ximo: */
			META_FINAL_GR_M1 = min(META_FINAL_GR_M1, 170000);
			META_FINAL_GR_M2 = min(META_FINAL_GR_M2, 170000);
		end;
	if VISAO_NEG = "SEL" and AGRUPA_SALDO = "CRESC_INVEST" then 
		do;			
			/* Aplicando MÌnimo :*/
			META_FINAL_GR_M1 = max(META_FINAL_GR_M1, 150000);
			META_FINAL_GR_M2 = max(META_FINAL_GR_M2, 150000);
			/* Aplicando M·ximo: */
			META_FINAL_GR_M1 = min(META_FINAL_GR_M1, 3200000);
			META_FINAL_GR_M2 = min(META_FINAL_GR_M2, 3200000);
		end;

/* Aplicando Meta MÌnima e M·xima p/ EprÈstimos FOco ... */	
	if VISAO_NEG = "E1" and AGRUPA_SALDO = "CRESC_EMPR_FOCO" then
		do;			
			/* Aplicando MÌnimo :*/
			META_FINAL_GR_M1 = max(META_FINAL_GR_M1, 10000);
			META_FINAL_GR_M2 = max(META_FINAL_GR_M2, 20000);
			/* Aplicando Ma¥ximo: */
			META_FINAL_GR_M1 = min(META_FINAL_GR_M1, 30000);
			META_FINAL_GR_M2 = min(META_FINAL_GR_M2, 70000);
		end; 

	if VISAO_NEG = "E2" and AGRUPA_SALDO = "CRESC_EMPR_FOCO" then
		do;			
			if FLAG_POLO = 0 then 
			do;
				/* Aplicando MÌnimo :*/
				META_FINAL_GR_M1 = max(META_FINAL_GR_M1, 25000);
				META_FINAL_GR_M2 = max(META_FINAL_GR_M2, 40000);
				/* Aplicando Ma¥ximo: */
				META_FINAL_GR_M1 = min(META_FINAL_GR_M1, 90000);
				META_FINAL_GR_M2 = min(META_FINAL_GR_M2, 150000);
			end;

			if FLAG_POLO = 1 then 
			do;
				/* Aplicando MÌnimo :*/
				META_FINAL_GR_M1 = max(META_FINAL_GR_M1, 40000);
				META_FINAL_GR_M2 = max(META_FINAL_GR_M2, 55000);
				/* Aplicando Ma¥ximo: */
				META_FINAL_GR_M1 = min(META_FINAL_GR_M1, 100000);
				META_FINAL_GR_M2 = min(META_FINAL_GR_M2, 180000);
			end;
		end; 
	if TIPO_PESSOA = "PF" and AGRUPA_SALDO = "CRESC_EMPR_FOCO" then
		do;			
			/* Aplicando MÌnimo :*/
			META_FINAL_GR_M1 = max(META_FINAL_GR_M1, 10000);
			META_FINAL_GR_M2 = max(META_FINAL_GR_M2, 10000);
			/* Aplicando Ma¥ximo: */
			META_FINAL_GR_M1 = min(META_FINAL_GR_M1, 150000);
			META_FINAL_GR_M2 = min(META_FINAL_GR_M2, 150000);
		end; 
	if VISAO_NEG = "CC" and AGRUPA_SALDO = "CRESC_EMPR_TOTAL" then
		do;			
			/* Aplicando MÌnimo :*/
			META_FINAL_GR_M1 = max(META_FINAL_GR_M1, 15000);
			META_FINAL_GR_M2 = max(META_FINAL_GR_M2, 15000);
			/* Aplicando Ma¥ximo: */
			META_FINAL_GR_M1 = min(META_FINAL_GR_M1, 150000);
			META_FINAL_GR_M2 = min(META_FINAL_GR_M2, 150000);
		end; 
	if VISAO_NEG = "VG"  and AGRUPA_SALDO = "CRESC_EMPR_TOTAL" then
		do;			
			/* Aplicando MÌnimo :*/
			META_FINAL_GR_M1 = max(META_FINAL_GR_M1, 15000);
			META_FINAL_GR_M2 = max(META_FINAL_GR_M2, 15000);
			/* Aplicando Ma¥ximo: */
			META_FINAL_GR_M1 = min(META_FINAL_GR_M1, 150000);
			META_FINAL_GR_M2 = min(META_FINAL_GR_M2, 150000);
		end; 
	if VISAO_NEG = "SEL" and AGRUPA_SALDO = "CRESC_EMPR_TOTAL" then
		do;			
			/* Aplicando MÌnimo :*/
			META_FINAL_GR_M1 = max(META_FINAL_GR_M1, 65000);
			META_FINAL_GR_M2 = max(META_FINAL_GR_M2, 65000);
			/* Aplicando Ma¥ximo: */
			META_FINAL_GR_M1 = min(META_FINAL_GR_M1, 300000);
			META_FINAL_GR_M2 = min(META_FINAL_GR_M2, 320000);
		end; 

	if VISAO_NEG = "NDA" and AGRUPA_SALDO = "CRESC_EMPR_FOCO" then
		do;			
			/* Aplicando M·ximo: */
			META_FINAL_GR_M1 = min(META_FINAL_GR_M1, 90000);
			META_FINAL_GR_M2 = min(META_FINAL_GR_M2, 150000);
		end; 


/* Aplicando Meta MÌnima e M·xima p/ EprÈstimos Total ... */	
	if VISAO_NEG = "E1" and AGRUPA_SALDO = "CRESC_EMPR_EX_FOCO" then
		do;			
			/* Aplicando MÌnimo :*/
			META_FINAL_GR_M1 = max(META_FINAL_GR_M1, 10000);
			META_FINAL_GR_M2 = max(META_FINAL_GR_M2, 20000);
			/* Aplicando Ma¥ximo: */
			META_FINAL_GR_M1 = min(META_FINAL_GR_M1, 35000);
			META_FINAL_GR_M2 = min(META_FINAL_GR_M2, 80000);

			AGRUPA_SALDO = "CRESC_EMPR_TOTAL";
		end; 

	if VISAO_NEG = "E2" and AGRUPA_SALDO = "CRESC_EMPR_EX_FOCO" then
		do;			
			if FLAG_POLO = 0 then 
			do;
				/* Aplicando MÌnimo :*/
				META_FINAL_GR_M1 = max(META_FINAL_GR_M1, 25000);
				META_FINAL_GR_M2 = max(META_FINAL_GR_M2, 40000);
				/* Aplicando Ma¥ximo: */
				META_FINAL_GR_M1 = min(META_FINAL_GR_M1, 90000);
				META_FINAL_GR_M2 = min(META_FINAL_GR_M2, 150000);
			end;

			if FLAG_POLO = 1 then 
			do;
				/* Aplicando MÌnimo :*/
				META_FINAL_GR_M1 = max(META_FINAL_GR_M1, 40000);
				META_FINAL_GR_M2 = max(META_FINAL_GR_M2, 55000);
				/* Aplicando Ma¥ximo: */
				META_FINAL_GR_M1 = min(META_FINAL_GR_M1, 100000);
				META_FINAL_GR_M2 = min(META_FINAL_GR_M2, 180000);
			end;

			AGRUPA_SALDO = "CRESC_EMPR_TOTAL";
		end; 

	if VISAO_NEG = "NDA" and AGRUPA_SALDO = "CRESC_EMPR_EX_FOCO" then
		do;			
			/* Aplicando M·ximo: */
			META_FINAL_GR_M1 = min(META_FINAL_GR_M1, 70000);
			META_FINAL_GR_M2 = min(META_FINAL_GR_M2, 130000);

			AGRUPA_SALDO = "CRESC_EMPR_TOTAL";
		end; 
run;
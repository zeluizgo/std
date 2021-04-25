/*   START OF NODE: Import Data (TB_JL_AUX_TESTE_PONDERA_VARS.xlsx[TB_AUX_TESTE_PONDERACOES])   */
%LET _CLIENTTASKLABEL='Import Data (TB_JL_AUX_TESTE_PONDERA_VARS.xlsx[TB_AUX_TESTE_PONDERACOES])';
%LET _CLIENTPROJECTPATH='C:\Users\T694592\Desktop\working_sas\Metas_Saldos_GR_v2.egp';
%LET _CLIENTPROJECTNAME='Metas_Saldos_GR_v2.egp';

GOPTIONS ACCESSIBLE;
/* --------------------------------------------------------------------
   Code generated by a SAS task
   
   Generated on Friday, April 28, 2017 at 8:19:50 AM
   By task:     Import Data Wizard
   
   Source file:
   C:\Users\T694592\Desktop\working_sas\TB_JL_AUX_TESTE_PONDERA_VARS.x
   lsx
   Server:      Local File System
   
   Output data: JOSELUIZ.TB_JL_AUX_TESTE_PONDERA_VARS
   Server:      SASApp_Incentivos
   
   Note: In preparation for running the following code, the Import
   Data wizard has used internal routines to transfer the source data
   file from the local file system to SASApp_Incentivos. There is no
   SAS code available to represent this action.
   -------------------------------------------------------------------- */

/* --------------------------------------------------------------------
   This DATA step reads the data values from a temporary text file
   created by the Import Data wizard. The values within the temporary
   text file were extracted from the Excel source file.
   -------------------------------------------------------------------- */

DATA JOSELUIZ.TB_JL_AUX_TESTE_PONDERA_VARS;
    LENGTH
        pond_var1          8
        pond_var2          8 ;
    FORMAT
        pond_var1        BEST12.
        pond_var2        BEST12. ;
    INFORMAT
        pond_var1        BEST12.
        pond_var2        BEST12. ;
    INFILE '/work/incentivos/SAS_work01BC00460134_spubd206/#LN00058'
        LRECL=22
        ENCODING="LATIN1"
        TERMSTR=CRLF
        DLM='7F'x
        MISSOVER
        DSD ;
    INPUT
        pond_var1        : BEST32.
        pond_var2        : BEST32. ;
RUN;


GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;


/*   START OF NODE: Aux Teste Ponderacoes   */
%LET _CLIENTTASKLABEL='Aux Teste Ponderacoes';
%LET _CLIENTPROJECTPATH='C:\Users\T694592\Desktop\working_sas\Metas_Saldos_GR_v2.egp';
%LET _CLIENTPROJECTNAME='Metas_Saldos_GR_v2.egp';
%LET _SASPROGRAMFILE=;

GOPTIONS ACCESSIBLE;


%MACRO testa_var_saldo_meta_prod(pond_var1,pond_var2);
data _null_;
put "TVAR: dtset_name=&pond_var1.";
put "TVAR: dtnok_name=&pond_var2.";
run;	

data testa_ponderacoes;
set joseluiz.tb_aux_pondera_meta_gr_all;
META_SALDO_M1_GR = META_SALDO_M1_AG*(PART_GR_SALDO*&pond_var1.+PART_GR_META_PROD_M1*&pond_var2.);
META_SALDO_M2_GR = META_SALDO_M2_AG*(PART_GR_SALDO*&pond_var1.+PART_GR_META_PROD_M2*&pond_var2.);
run;

/*
data testa_ponderacoes;
set joseluiz.tb_aux_pondera_meta_gr_all;
META_SALDO_M1_GR = META_SALDO_M1_AG*(PART_GR_SALDO*0+PART_GR_META_PROD_M1*1);
META_SALDO_M2_GR = META_SALDO_M2_AG*(PART_GR_SALDO*0+PART_GR_META_PROD_M2*1);
run;
*/

proc sql;
create table tb_aux_result_ponderacoes as
select "SALDO" as var1, &pond_var1. as pond_var1, "META_PROD" as var2, &pond_var2. as pond_var2, 
TIPO_PESSOA	, AGRUPA_SALDO, VISAO_NEG,
min(META_SALDO_M1_GR) as minimo_GR_M1, AVG(META_SALDO_M1_GR)  as media_GR_M1, MAX(META_SALDO_M1_GR) as maximo_GR_M1, STD(META_SALDO_M1_GR) as desvio_GR_M1, case when AVG(META_SALDO_M1_GR) <> 0 then STD(META_SALDO_M1_GR)/AVG(META_SALDO_M1_GR) else 0 end as normal_GR_M1,
       min(META_SALDO_M2_GR) as minimo_GR_M2, AVG(META_SALDO_M2_GR)  as media_GR_M2, MAX(META_SALDO_M2_GR) as maximo_GR_M2, STD(META_SALDO_M2_GR) as desvio_GR_M2, case when AVG(META_SALDO_M2_GR) <> 0 then STD(META_SALDO_M2_GR)/AVG(META_SALDO_M2_GR) else 0 end as normal_GR_M2
from testa_ponderacoes
group by var1, pond_var1, var2, pond_var2, TIPO_PESSOA	, AGRUPA_SALDO, VISAO_NEG;
quit;


proc append base=joseluiz.TB_RESULT_TEST_PONDERACOES data=tb_aux_result_ponderacoes force; quit;

%MEND testa_var_saldo_meta_prod;





data _null_;
set JOSELUIZ.TB_JL_AUX_TESTE_PONDERA_VARS;
call execute('%testa_var_saldo_meta_prod('||pond_var1||','||pond_var2||')');
run;



data testa_ponderacoes;
set joseluiz.tb_aux_pondera_meta_gr_all;
META_SALDO_M1_GR = META_SALDO_M1_AG*PART_GR_META_BETA_M1;
META_SALDO_M2_GR = META_SALDO_M2_AG*PART_GR_META_BETA_M2;
run;


proc sql;
create table tb_aux_result_ponderacoes as
select "META_BETA" as var1, 1 as pond_var1,  
TIPO_PESSOA	, AGRUPA_SALDO, VISAO_NEG,
min(META_SALDO_M1_GR) as minimo_GR_M1, AVG(META_SALDO_M1_GR)  as media_GR_M1, MAX(META_SALDO_M1_GR) as maximo_GR_M1, STD(META_SALDO_M1_GR) as desvio_GR_M1, case when AVG(META_SALDO_M1_GR) <> 0 then STD(META_SALDO_M1_GR)/AVG(META_SALDO_M1_GR) else 0 end as normal_GR_M1,
       min(META_SALDO_M2_GR) as minimo_GR_M2, AVG(META_SALDO_M2_GR)  as media_GR_M2, MAX(META_SALDO_M2_GR) as maximo_GR_M2, STD(META_SALDO_M2_GR) as desvio_GR_M2, case when AVG(META_SALDO_M2_GR) <> 0 then STD(META_SALDO_M2_GR)/AVG(META_SALDO_M2_GR) else 0 end as normal_GR_M2
from testa_ponderacoes 
group by var1, pond_var1, TIPO_PESSOA	, AGRUPA_SALDO, VISAO_NEG;
quit;


proc append base=joseluiz.TB_RESULT_TEST_PONDERACOES data=tb_aux_result_ponderacoes force; quit;










/*
*/

/*
data joseluiz.TB_RESULT_TEST_PONDERACOES;
set joseluiz.TB_RESULT_TEST_PONDERACOES;
delete;
run;
format VISAO_NEG	$CHAR3.;
*/



GOPTIONS NOACCESSIBLE;
%LET _CLIENTTASKLABEL=;
%LET _CLIENTPROJECTPATH=;
%LET _CLIENTPROJECTNAME=;
%LET _SASPROGRAMFILE=;


/*   START OF NODE: Import Data (TB_JL_UNIDADES_POLOS.xlsx[TB_JL_UNIDADES_POLOS])   */
%LET _CLIENTTASKLABEL='Import Data (TB_JL_UNIDADES_POLOS.xlsx[TB_JL_UNIDADES_POLOS])';
%LET _CLIENTPROJECTPATH='C:\Users\T694592\Desktop\working_sas\Metas_Saldos_GR_v2.egp';
%LET _CLIENTPROJECTNAME='Metas_Saldos_GR_v2.egp';

GOPTIONS ACCESSIBLE;
/* --------------------------------------------------------------------
   Code generated by a SAS task
   
   Generated on Wednesday, May 3, 2017 at 12:20:40 PM
   By task:     Import Data Wizard
   
   Source file:
   C:\Users\T694592\Desktop\working_sas\TB_JL_UNIDADES_POLOS.xlsx
   Server:      Local File System
   
   Output data: JOSELUIZ.TB_JL_UNIDADES_POLOS
   Server:      SASApp_Incentivos
   
   Note: In preparation for running the following code, the Import
   Data wizard has used internal routines to transfer the source data
   file from the local file system to SASApp_Incentivos. There is no
   SAS code available to represent this action.
   -------------------------------------------------------------------- */

/* --------------------------------------------------------------------
   This DATA step reads the data values from a temporary text file
   created by the Import Data wizard. The values within the temporary
   text file were extracted from the Excel source file.
   -------------------------------------------------------------------- */

DATA JOSELUIZ.TB_JL_UNIDADES_POLOS;
    LENGTH
        UNIORG_POLO        8 ;
    FORMAT
        UNIORG_POLO      BEST12. ;
    INFORMAT
        UNIORG_POLO      BEST12. ;
    INFILE '/work/incentivos/SAS_workE6DA01D9004E_spubd206/#LN00062'
        LRECL=5
        ENCODING="LATIN1"
        TERMSTR=CRLF
        DLM='7F'x
        MISSOVER
        DSD ;
    INPUT
        UNIORG_POLO      : BEST32. ;
RUN;

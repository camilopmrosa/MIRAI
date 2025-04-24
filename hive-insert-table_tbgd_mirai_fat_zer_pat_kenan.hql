-- ***********************************************************************************************************************************
-- Nome do Projeto   : Mirai
-- Alarme            : Fatura Zerada - Patamar
-- Descrição         : 
--                   : 
-- Autor             : 
-- Data de Criação   : 
-- Última Modificação: 20/03/2025
-- Versão            : 1.0
-- Observações       : Tabela Analitica: h_bigd_db.tbgd_mirai_ftrm_fatura_zerada / Tabela Alarme Final: h_bigd_db.tbgd_mirai_alrm_fnl
-- Ferramenta        : Hive / Shell
-- ***********************************************************************************************************************************
-- Histórico de Modificações:
-- Data                    Alteração                        Autor
-- [Data da Modificação]  [Descrição da Modificação]       [Nome do Autor da Modificação]
-- ***********************************************************************************************************************************


--CARGA DE FONTES--------------------------------------------------------------------------------------------------------------------------------
--TABELA DE CLIENTE---------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_cliente_proc1;

CREATE TABLE IF NOT EXISTS h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_cliente_proc1
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY', 'transactional'='false')
AS
SELECT 
    account_no as account_no,
    cust_state as cust_state,
    --account_category as account_category,
    --no_bill as no_bill,
    --remark as remark,
    CASE WHEN account_category IN ('12', '13', '14', '15', '21', '22') THEN 'B2B'
            WHEN account_category IN ('17','18','19','20') THEN  'CONTAS N FATURAVEIS'
            WHEN account_category IN ('16') THEN 'USO PROPRIO'
            ELSE 'B2C' 
    END AS tipo_cliente,
    count(*) as qtde
FROM p_bigd_camada_semantica_db.tbgd_tfat_mvel_clnt_kennan
group by account_no,
         cust_state,
         CASE WHEN account_category IN ('12', '13', '14', '15', '21', '22') THEN 'B2B'
            WHEN account_category IN ('17','18','19','20') THEN  'CONTAS N FATURAVEIS'
            WHEN account_category IN ('16') THEN 'USO PROPRIO'
            ELSE 'B2C' 
         END
;

--TABELA DE FATURAMENTO INICIAL------------------------------------------------------------------------------------35.822.287
DROP TABLE IF EXISTS h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_fat_proc1;

CREATE TABLE IF NOT EXISTS h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_fat_proc1
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY', 'transactional'='false')
AS
SELECT
    DATE_FORMAT(f.prep_date, 'yyyyMM') as prep_date,
    f.bill_ref_no                      as bill_ref_no,
    f.account_no                       as account_no,
    sum(f.amount)                      as sum_amount,
    sum(f.tax)                         as sum_tax,
    sum(f.discount)                    as sum_discount
FROM p_bigd_camada_semantica_db.tbgd_tfat_mvel_ftrm_kennan f
WHERE
    f.prep_status = '1'
    AND f.TYPE_CODE IN (2,3,6,7)--AVALIAR A QUEST O DE FRANQUIA/USOS NO CALCULO
    AND DATE_FORMAT(f.prep_date, 'yyyyMM') >= DATE_FORMAT(add_months(current_date,-7),'yyyyMM')
    AND DATE_FORMAT(f.prep_date, 'yyyyMM') <= DATE_FORMAT(add_months(current_date,-1),'yyyyMM')
    AND f.backout_status = '0'
    AND (prep_error_code is null OR prep_error_code = 0)
group by DATE_FORMAT(f.prep_date, 'yyyyMM'),
    f.bill_ref_no,
    f.account_no
;

--IMPOSTOS (EM DESENVOLVIMENTO)--------------------------------------------------------------------------------------
DROP TABLE IF EXISTS h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_impostos_proc1;
CREATE  TABLE h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_impostos_proc1
AS

select a.bill_ref_no, 
       --a.bill_invoice_row, 
       CAST(round(sum(a.federal_tax/100),2) as decimal(29,2)) as tax
from   p_bigd_kennanesiebel_db.tbgdt_kennanesiebel_arbor_pbct2_bill_invoice_tax a
       JOIN (select max(dt_foto) as dt_foto from p_bigd_kennanesiebel_db.tbgdt_kennanesiebel_arbor_pbct2_bill_invoice_tax) b
            ON a.dt_foto = b.dt_foto
       JOIN (select distinct bill_ref_no as bill_ref_no from p_bigd_camada_semantica_db.tbgd_tfat_mvel_ftrm_kennan) c
            ON a.bill_ref_no = c.bill_ref_no
where tax_type_code >= 1000000
and tax_type_code < 4000000
--where bill_ref_no = '1818255699'
and federal_tax > 0
group by a.bill_ref_no--, 
--a.bill_invoice_row
UNION ALL
select a.bill_ref_no, 
       --a.bill_invoice_row, 
       CAST(round(sum(a.federal_tax/100),2) as decimal(29,2)) as tax
from   p_bigd_kennanesiebel_db.tbgdt_kennanesiebel_arbor_pbct1_bill_invoice_tax a
       JOIN (select max(dt_foto) as dt_foto from p_bigd_kennanesiebel_db.tbgdt_kennanesiebel_arbor_pbct1_bill_invoice_tax) b
            ON a.dt_foto = b.dt_foto
       JOIN (select distinct bill_ref_no as bill_ref_no from p_bigd_camada_semantica_db.tbgd_tfat_mvel_ftrm_kennan) c
            ON a.bill_ref_no = c.bill_ref_no
where tax_type_code >= 1000000
and tax_type_code < 4000000
--where bill_ref_no = '1818255699'
and federal_tax > 0
group by a.bill_ref_no--, 
--a.bill_invoice_row
;

--UNIR FATURAMENTO x IMPOSTOS
DROP TABLE IF EXISTS h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_pre_fat_e_cli;
CREATE  TABLE h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_pre_fat_e_cli
AS

select a.prep_date,
       a.bill_ref_no,
       a.account_no,
       a.sum_amount,
       CASE WHEN b.tax IS NULL THEN 0
            WHEN b.tax = 'NULL' THEN 0
            ELSE b.tax
       END       as sum_tax,
       a.sum_discount

from h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_fat_proc1 a
     left join h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_impostos_proc1 b
          ON a.bill_ref_no = b.bill_ref_no
--limit 10
;

--FATURAMENTO x CLIENTE PRINCIPAL----------------------------------------------------------------------------------------
DROP TABLE IF EXISTS h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_fat_e_cli;

CREATE TABLE IF NOT EXISTS h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_fat_e_cli
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY', 'transactional'='false')
AS
select f.prep_date,
       f.bill_ref_no,
       f.account_no,
       g.cust_state,
       g.tipo_cliente,
       f.sum_amount,
       f.sum_tax,
       f.sum_discount
from h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_pre_fat_e_cli f
     LEFT JOIN h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_cliente_proc1 g
         ON f.account_no = g.account_no
;
--FIM CARGA DE FONTES----------------------------------------------------------------------------------------------------------------------------



--BASES E REGRAS DE NEGOCIO----------------------------------------------------------------------------------------------------------------------

------------------------------------------------------------------------
--FATURA ZERADA---------------------------------------------------------
------------------------------------------------------------------------

--TABELA FONTE - FATURA ZERADA (PATAMAR E REGRA)-----------------------------------------------------------------------------
DROP TABLE IF EXISTS h_bigd_db.tbgd_mirai_ftrm_fatura_zerada;

CREATE TABLE IF NOT EXISTS h_bigd_db.tbgd_mirai_ftrm_fatura_zerada
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY', 'transactional'='false')
AS
select prep_date, 
       bill_ref_no, 
       account_no, 
       cust_state, 
       tipo_cliente, 
       sum_amount, 
       sum_tax, 
       sum_discount
from h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_fat_e_cli
where sum_amount = 0
;


--TABELA FONTE - FATURA ZERADA (PATAMAR E REGRA) - CONTADORES------------------------------------------------------------------
DROP TABLE IF EXISTS h_bigd_db.tbgd_mirai_ftrm_fatura_zerada_contador;

CREATE TABLE IF NOT EXISTS h_bigd_db.tbgd_mirai_ftrm_fatura_zerada_contador
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY', 'transactional'='false')
AS
select CASE WHEN tipo_cliente IS NULL THEN 'B2C' ELSE tipo_cliente END as tipo_cliente,
       cust_state,
       bill_ref_no,
--       MAX(CASE WHEN prep_date = DATE_FORMAT(add_months(current_date,-7-1),'yyyyMM') THEN sum_amount ELSE NULL END) as MES_1,
--       MAX(CASE WHEN prep_date = DATE_FORMAT(add_months(current_date,-6-1),'yyyyMM') THEN sum_amount ELSE NULL END) as MES_2,
--       MAX(CASE WHEN prep_date = DATE_FORMAT(add_months(current_date,-5-1),'yyyyMM') THEN sum_amount ELSE NULL END) as MES_3,
--       MAX(CASE WHEN prep_date = DATE_FORMAT(add_months(current_date,-4-1),'yyyyMM') THEN sum_amount ELSE NULL END) as MES_4,
--       MAX(CASE WHEN prep_date = DATE_FORMAT(add_months(current_date,-3-1),'yyyyMM') THEN sum_amount ELSE NULL END) as MES_5,
--       MAX(CASE WHEN prep_date = DATE_FORMAT(add_months(current_date,-2-1),'yyyyMM') THEN sum_amount ELSE NULL END) as MES_6,
--       MAX(CASE WHEN prep_date = DATE_FORMAT(add_months(current_date,-1-1),'yyyyMM') THEN sum_amount ELSE NULL END) as MES_ATUAL,

       MAX(CASE WHEN prep_date = DATE_FORMAT(add_months(current_date,-7),'yyyyMM') THEN sum_amount ELSE NULL END) as MES_6,
       MAX(CASE WHEN prep_date = DATE_FORMAT(add_months(current_date,-6),'yyyyMM') THEN sum_amount ELSE NULL END) as MES_5,
       MAX(CASE WHEN prep_date = DATE_FORMAT(add_months(current_date,-5),'yyyyMM') THEN sum_amount ELSE NULL END) as MES_4,
       MAX(CASE WHEN prep_date = DATE_FORMAT(add_months(current_date,-4),'yyyyMM') THEN sum_amount ELSE NULL END) as MES_3,
       MAX(CASE WHEN prep_date = DATE_FORMAT(add_months(current_date,-3),'yyyyMM') THEN sum_amount ELSE NULL END) as MES_2,
       MAX(CASE WHEN prep_date = DATE_FORMAT(add_months(current_date,-2),'yyyyMM') THEN sum_amount ELSE NULL END) as MES_1,
       MAX(CASE WHEN prep_date = DATE_FORMAT(add_months(current_date,-1),'yyyyMM') THEN sum_amount ELSE NULL END) as MES_ATUAL,
       count(*) QTDE 

from h_bigd_db.tbgd_mirai_ftrm_fatura_zerada
group by tipo_cliente,
       cust_state,
       bill_ref_no
;


--PATAMAR-----------------------------------------------------------------------------------------------------------------------
--DROP TABLE IF EXISTS h_bigd_db.tbgd_mirai_alrm_sp4_fat_zer_pat; --TROCAR O DROP E CREATE POR INSERT NA VERS�O FINAL
--
--CREATE TABLE IF NOT EXISTS h_bigd_db.tbgd_mirai_alrm_sp4_fat_zer_pat --TROCAR O DROP E CREATE POR INSERT NA VERS�O FINAL
--STORED AS ORC --TROCAR O DROP E CREATE POR INSERT NA VERS�O FINAL
--TBLPROPERTIES ('orc.compress'='SNAPPY', 'transactional'='false') --TROCAR O DROP E CREATE POR INSERT NA VERS�O FINAL
--AS --TROCAR O DROP E CREATE POR INSERT NA VERS�O FINAL


INSERT INTO h_bigd_db.tbgd_mirai_alrm_fnl 
(

select

a.tipo_cliente                                                                          as segmento,
'SEM INFO'                                                                          as tipo_cliente,
CASE WHEN a.cust_state IS NULL THEN 'SEM UF'
     WHEN a.cust_state = 'NULL' THEN 'SEM UF' 
     ELSE a.cust_state 
END                                                                                     as item_alarme,
CASE WHEN a.cust_state IS NULL THEN 'SEM UF'
     WHEN a.cust_state = 'NULL' THEN 'SEM UF' 
     ELSE a.cust_state 
END                                                                                     as produto,
'SEM CLASSIFICACAO'                                                                     as tipo_fat,
DATE_FORMAT(add_months(current_date,-1),'yyyyMM')                                       as periodo,
count(mes_atual)                                                                        as valor,


--[x] Colocar campos novos na query
--[x] Setar Formula baseado no excel

CASE 
     WHEN 0 = 0 OR 0 IS NULL THEN 0.00 -- WHEN media_meses = 0 OR 0 IS NULL THEN 0 
     ELSE ROUND(ABS((count(mes_atual)   / 0) - 1),2) -- ELSE ABS((valor  / media_meses) - 1) 
END                                                                                     AS media_valor,
0                                                                                       as media_quant,
0                                                                                       as dif_media_valor_x_quant,
---

ROUND((count(mes_atual)/count(mes_1))-1,2)                                              as var_mes_ant_1,
count(mes_1)                                                                            as valor_mes_ant_1, --MES ATUAL -1
ROUND((count(mes_1)/count(mes_2))-1,2)                                                  as var_mes_ant_2,
count(mes_2)                                                                            as valor_mes_ant_2,
ROUND((count(mes_2)/count(mes_3))-1,2)                                                  as var_mes_ant_3,
count(mes_3)                                                                            as valor_mes_ant_3,
ROUND((count(mes_3)/count(mes_4))-1,2)                                                  as var_mes_ant_4,
count(mes_4)                                                                            as valor_mes_ant_4,
ROUND((count(mes_4)/count(mes_5))-1,2)                                                  as var_mes_ant_5,
count(mes_5)                                                                            as valor_mes_ant_5,
ROUND((count(mes_5)/count(mes_6))-1,2)                                                  as var_mes_ant_6,
count(mes_6)                                                                            as valor_mes_ant_6,

ROUND(ABS((NVL((count(mes_atual)/count(mes_1))-1,0))+
 ABS(NVL((count(mes_1)/count(mes_2))-1,0))+
 ABS(NVL((count(mes_2)/count(mes_3))-1,0))+
 ABS(NVL((count(mes_3)/count(mes_4))-1,0))+
 ABS(NVL((count(mes_4)/count(mes_5))-1,0))+
 ABS(NVL((count(mes_5)/count(mes_6))-1,0))
)/6,2)                                                                                  as med_var_mes, --EDITAR---------------------------------------------------------------


b.cod                                                                                   as cod,
b.tipo                                                                                  as tipo,
'Fatura Zerada - Patamar'                                                               as assunto,
'n'                                                                                     as redutor,
b.sistema                                                                               as sistema,
b.etapa                                                                                 as etapa,
ROUND((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6,2)       as media_meses,
ROUND(((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)-
((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)*b.fator_perc_inf,2)       as limite_inferior,
ROUND(((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)+
((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)*b.fator_perc_sup,2)       as limite_superior,

0                                                                                       as imp_x_fat_atual,
0                                                                                       as imp_x_fat_media,


b.fator_perc_inf                                                                        as fator_perc_inf,
b.fator_perc_sup                                                                        as fator_perc_sup,
b.fator_num_inf                                                                         as fator_num_inf, --AQUI, TEMOS QUE VER A QUEST O DE COLOCAR UMA REGRA DE MINIMO DE PROBLEMA
b.fator_num_sup                                                                         as fator_num_sup, --AQUI, TEMOS QUE VER A QUEST O DE COLOCAR UMA REGRA DE MINIMO DE PROBLEMA
b.fator_perc_composto                                                                   as fator_perc_composto,


CASE WHEN count(mes_atual) < (((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)-
((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)*b.fator_perc_inf) THEN 's'
     WHEN count(mes_atual) > (((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)+
((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)*b.fator_perc_sup) THEN 's'
     ELSE 'n'                                                                      
END                                                                                     as flag_alarme,

CASE WHEN count(a.mes_atual) < (((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)-
((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)*b.fator_perc_inf) THEN 'Normal'
     WHEN count(a.mes_atual) > (((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)+
((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)*b.fator_perc_sup) THEN 'Ultrapassou Limite Superior'
     ELSE 'Normal'                                                                      
END                                                                                     as alarme,

b.ponto                                                                                 as ponto,
'Quantidade Fatura'                                                                          as referencia_valor,
'Faturamento - Outros'                                                                  as rel_ind_produto,
'SEM CLASS'                                                                             as tecnologia, --COLOCAr os dados de tecnologia
current_timestamp()                                                                     as data_alarme,
0                                                                                       as quant,
0                                                                                       as quant_mes_ant,
0                                                                                       as quant_med,
0                                                                                       as limite_inferior_quant,
0                                                                                       as limite_superior_quant,
0                                                                                       as fator_perc_inf_quant,
0                                                                                       as fator_perc_sup_quant,
0                                                                                       as arpu,
0                                                                                       as arpu_mes_ant,
'MENSAL'                                                                                as periodicidade


from h_bigd_db.tbgd_mirai_ftrm_fatura_zerada_contador a
     LEFT JOIN (select * from h_bigd_db.tbgdt_fator_alarm
      where cod >= 'PAT0501401'
      and cod <= 'PAT0501700'
      and campo = 'ESTADO') b
          ON a.tipo_cliente = b.tipo_cliente
          AND (CASE WHEN a.cust_state IS NULL THEN 'SEM UF'
                   WHEN a.cust_state = 'NULL' THEN 'SEM UF' 
                   ELSE a.cust_state 
              END) = b.chave
          
group by a.tipo_cliente,
         a.cust_state,
         b.cod,
         b.tipo,
         b.sistema,
         b.etapa,
         b.fator_perc_inf,
         b.fator_perc_sup,
         b.fator_num_inf,
         b.fator_num_sup,
         fator_perc_composto,
         b.ponto
         
         
UNION ALL


select

a.tipo_cliente                                                                          as segmento,
'SEM INFO'                                                                          as tipo_cliente,
'PERIODO'                                                                               as item_alarme,
'PERIODO'                                                                               as produto,
'SEM CLASSIFICACAO'                                                                     as tipo_fat,
DATE_FORMAT(add_months(current_date,-1),'yyyyMM')                                       as periodo,             
count(mes_atual)                                                                        as valor,

--[x] Colocar campos novos na query
--[x] Setar Formula baseado no excel

CASE 
     WHEN 0 = 0 OR 0 IS NULL THEN 0.00 -- WHEN media_meses = 0 OR 0 IS NULL THEN 0 
     ELSE ROUND(ABS((count(mes_atual)  / 0) - 1),2) -- ELSE ABS((valor  / media_meses) - 1) 
END                                                                                     AS media_valor,
0                                                                                       as media_quant,
0                                                                                       as dif_media_valor_x_quant,
ROUND((count(mes_atual)/count(mes_1))-1,2)                                              as var_mes_ant_1,
count(mes_1)                                                                            as valor_mes_ant_1, --MES ATUAL -1
ROUND((count(mes_1)/count(mes_2))-1,2)                                                  as var_mes_ant_2,
count(mes_2)                                                                            as valor_mes_ant_2,
ROUND((count(mes_2)/count(mes_3))-1,2)                                                  as var_mes_ant_3,
count(mes_3)                                                                            as valor_mes_ant_3,
ROUND((count(mes_3)/count(mes_4))-1,2)                                                  as var_mes_ant_4,
count(mes_4)                                                                            as valor_mes_ant_4,
ROUND((count(mes_4)/count(mes_5))-1,2)                                                  as var_mes_ant_5,
count(mes_5)                                                                            as valor_mes_ant_5,
ROUND((count(mes_5)/count(mes_6))-1,2)                                                  as var_mes_ant_6,
count(mes_6)                                                                            as valor_mes_ant_6,

ROUND(ABS((NVL((count(mes_atual)/count(mes_1))-1,0))+
 ABS(NVL((count(mes_1)/count(mes_2))-1,0))+
 ABS(NVL((count(mes_2)/count(mes_3))-1,0))+
 ABS(NVL((count(mes_3)/count(mes_4))-1,0))+
 ABS(NVL((count(mes_4)/count(mes_5))-1,0))+
 ABS(NVL((count(mes_5)/count(mes_6))-1,0))
)/6,2)
                                                                                        as med_var_mes, --EDITAR---------------------------------------------------------------


b.cod                                                                                   as cod,
b.tipo                                                                                  as tipo,
'Fatura Zerada - Patamar'                                                               as assunto,
'n'                                                                                     as redutor,
b.sistema                                                                               as sistema,
b.etapa                                                                                 as etapa,
ROUND((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6,2)       as media_meses,
ROUND(((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)-
((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)*b.fator_perc_inf,2)       as limite_inferior,
ROUND(((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)+
((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)*b.fator_perc_sup,2)       as limite_superior,
0                                                                                       as imp_x_fat_atual,
0                                                                                       as imp_x_fat_media,


b.fator_perc_inf                                                                        as fator_perc_inf,
b.fator_perc_sup                                                                        as fator_perc_sup,
b.fator_num_inf                                                                         as fator_num_inf,
b.fator_num_sup                                                                         as fator_num_sup,
b.fator_perc_composto                                                                   as fator_perc_composto,


CASE WHEN count(mes_atual) < (((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)-
((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)*b.fator_perc_inf) THEN 's'
     WHEN count(mes_atual) > (((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)+
((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)*b.fator_perc_sup) THEN 's'
     ELSE 'n'                                                                      
END                                                                                     as flag_alarme,

CASE WHEN count(a.mes_atual) < (((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)-
((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)*b.fator_perc_inf) THEN 'Normal'
     WHEN count(a.mes_atual) > (((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)+
((count(mes_1)+count(mes_2)+count(mes_3)+count(mes_4)+count(mes_5)+count(mes_6))/6)*b.fator_perc_sup) THEN 'Ultrapassou Limite Superior'
     ELSE 'Normal'                                                                      
END                                                                                     as alarme,

b.ponto                                                                                 as ponto,
'Quantidade Fatura'                                                                          as referencia_valor,
'Faturamento - Outros'                                                                  as rel_ind_produto,
'SEM CLASS'                                                                             as tecnologia,
current_timestamp()                                                                     as data_alarme,
0                                                                                       as quant,
0                                                                                       as quant_mes_ant,
0                                                                                       as quant_med,
0                                                                                       as limite_inferior_quant,
0                                                                                       as limite_superior_quant,
0                                                                                       as fator_perc_inf_quant,
0                                                                                       as fator_perc_sup_quant,
0                                                                                       as arpu,
0                                                                                       as arpu_mes_ant,
'MENSAL'                                                                                as periodicidade


from h_bigd_db.tbgd_mirai_ftrm_fatura_zerada_contador a
     LEFT JOIN (select * from h_bigd_db.tbgdt_fator_alarm
      where cod >= 'PAT0501401'
      and cod <= 'PAT0501700'
      and campo = 'PERIODO') b
          ON a.tipo_cliente = b.tipo_cliente
group by a.tipo_cliente,
         b.cod,
         b.tipo,
         b.sistema,
         b.etapa,
         b.fator_perc_inf,
         b.fator_perc_sup,
         b.fator_num_inf,
         b.fator_num_sup,
         fator_perc_composto,
         b.ponto

);
--FIM PATAMAR-------------------------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_cliente_proc1;
DROP TABLE IF EXISTS h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_fat_proc1;
DROP TABLE IF EXISTS h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_impostos_proc1;
DROP TABLE IF EXISTS h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_pre_fat_e_cli;
DROP TABLE IF EXISTS h_bigd_db.tmp_tbgd_tfat_mvel_clnt_kennan_fat_e_cli;
DROP TABLE IF EXISTS h_bigd_db.tbgd_mirai_ftrm_fatura_zerada_contador;

-- ***********************************************************************************************************************************
-- Nome do Projeto   : Mirai
-- Alarme            : 
-- Descrição         : A&C Patamar  Ajuste e Contestação
-- Autor             : Everton Itamar Werka Pereira
-- Data de Criação   : 06/05/2025
-- Última Modificação: -
-- Versão            : 1.0
-- Observações       : -
-- Ferramenta        : Hive / Shell
-- ***********************************************************************************************************************************
-- Histórico de Modificações: Alteração da Carga de Clientes para pegar dados historicos que não temos na Camada Semantica - Carga atual vai ficar comentada
-- Data                    Alteração                        Autor
-- 22/04/2025              Alteração Carga                  Everton Itamar Werka Pereira
-- ***********************************************************************************************************************************

--CONFIGURACAO---------------------------------------------------------------------------------------------------
SET hive.merge.tezfiles=true; --otimizar o desempenho do Hive ao lidar com grandes conjuntos de dados, reduzindo o tempo de execução de consultas
SET hive.merge.mapfiles = true; --evitar a criação de muitos arquivos pequenos
SET hive.merge.mapredfiles = true; --combinar arquivos pequenos em um maior
SET hive.merge.size.per.task = 134217728; --orientação VIVO para tamanho
SET hive.merge.smallfiles.avgsize = 134217728; --orientação VIVO para tamanho
SET hive.exec.compress.output = true; --compactar as saídas pra diminuir o espaço em disco
--FIM CONFIGURACAO-----------------------------------------------------------------------------------------------


-----------------------------------------------------------------------------------------------------------------
--CONSULTA PRINCIPAL DE AJUSTES
-----------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS h_garantiareceita_db.ATLYS_FAT_SP17_AJUSTE; --NOME EXEMPLO (ALTERAR NA VERS O OFICIAL)

CREATE TABLE IF NOT EXISTS h_garantiareceita_db.ATLYS_FAT_SP17_AJUSTE --NOME EXEMPLO (ALTERAR NA VERS O OFICIAL)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY', 'transactional'='false')
AS

select    A.acct_nbr                                                as acct_nbr,
          substr(A.adj_date,1,6)                                    as adj_date,
          A.fncl_acct_nbr                                           as fncl_acct_nbr,
          A.adj_cr_debit_ind                                        as adj_cr_debit_ind,
          --A.fncl_pbln_id                                          as fncl_pbln_id,
          sum(A.adj_amt)                                            as adj_amt,
          --sum(b.credit_asgm_amt)                                  as credit_asgm_amt,
          A.reason_cd                                               as reason_cd,
          REPLACE(A.adj_custom_bill_desc,'|',' X ')                 as adj_custom_bill_desc,
          A.sbscrp_id                                               as sbscrp_id,
          --A.adj_tax_id                                            as adj_tax_id,
          A.fncl_trnsct_type_id                                     as fncl_trnsct_type_id,
          a.rvrs_dt                                                 as rvrs_dt,
          --A.person_id                                             as person_id,
          sum(A.cr_rmng_amt)                                        as cr_rmng_amt,
          --A.cr_active_asgm_qty                                    as cr_active_asgm_qty,
          --A.fncl_period_start_dt                                  as fncl_period_start_dt,
          --A.fncl_carr_cd                                          as fncl_carr_cd,
          f.fncl_trnsct_type_name                                   as fncl_trnsct_type_name,
          f.cr_debit_bal_ind                                        as cr_debit_bal_ind,
          f.pymt_adj_ind                                            as pymt_adj_ind,
          f.dbl_cr_flag                                             as dbl_cr_flag,
          A.dt_foto                                                 as dt_foto

from (select * from p_bigd_db.tbgdt_atlys_bsv_adj where dt_foto = (select max(dt_foto) from p_bigd_db.tbgdt_atlys_bsv_adj )) a
   
     --left join (select * from p_bigd_db.tbgdt_atlys_bsv_credit_asgm where dt_foto = '20241124') b
          --ON a.ACCT_NBR = b.ADJ_ACCT_NBR
          --AND a.ADJ_DATE = b.ADJ_DATE
          --AND a.ADJ_TM = b.ADJ_TM

     left join (select * from p_bigd_db.tbgd_tatlys_fncl_trnsct_type where dt_foto = (select max(dt_foto) from p_bigd_db.tbgd_tatlys_fncl_trnsct_type )) f      
          ON A.fncl_trnsct_type_id = f.fncl_trnsct_type_id
          
WHERE SUBSTR(a.adj_date,1,6) >= DATE_FORMAT(add_months(current_date,-7),'YYYYMM')
AND SUBSTR(a.adj_date,1,6) <= DATE_FORMAT(add_months(current_date,-1),'YYYYMM') 
and (a.rvrs_dt is null OR a.rvrs_dt = '' OR UPPER(a.rvrs_dt) = 'NULL')
--and A.fncl_trnsct_type_id = '50'
group by  A.acct_nbr,
          substr(A.adj_date,1,6),
          A.fncl_acct_nbr,
          A.adj_cr_debit_ind,
          --A.fncl_pbln_id                                            as fncl_pbln_id,
          A.reason_cd,
          REPLACE(A.adj_custom_bill_desc,'|',' X '),
          A.sbscrp_id,
          --A.adj_tax_id                                              as adj_tax_id,
          A.fncl_trnsct_type_id,
          a.rvrs_dt,
          --A.person_id                                               as person_id,
          --A.cr_rmng_amt                                             as cr_rmng_amt,
          --A.cr_active_asgm_qty                                      as cr_active_asgm_qty,
          --A.fncl_period_start_dt                                    as fncl_period_start_dt,
          --A.fncl_carr_cd                                            as fncl_carr_cd,
          f.fncl_trnsct_type_name,
          f.cr_debit_bal_ind,
          f.pymt_adj_ind,
          f.dbl_cr_flag,
          A.dt_foto
--limit 5
;
-----------------------------------------------------------------------------------------------------------------
--FIM CONSULTA PRINCIPAL DE AJUSTES
-----------------------------------------------------------------------------------------------------------------


-----------------------------------------------------------------------------------------------------------------
--AJUSTES x CLIENTES
-----------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS h_garantiareceita_db.tbgd_mirai_atlys_anlt_ajust_contest; --NOME EXEMPLO (ALTERAR NA VERS O OFICIAL)

CREATE TABLE IF NOT EXISTS h_garantiareceita_db.tbgd_mirai_atlys_anlt_ajust_contest --NOME EXEMPLO (ALTERAR NA VERS O OFICIAL)
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY', 'transactional'='false')
AS

select    A.acct_nbr                                                            as acct_nbr,
          A.adj_date                                                            as adj_date,
          A.fncl_acct_nbr                                                       as fncl_acct_nbr,
          A.adj_cr_debit_ind                                                    as adj_cr_debit_ind,
          A.adj_amt                                                             as adj_amt,
          A.reason_cd                                                           as reason_cd,
          A.adj_custom_bill_desc                                                as adj_custom_bill_desc,
          A.sbscrp_id                                                           as sbscrp_id,
          A.fncl_trnsct_type_id                                                 as fncl_trnsct_type_id,
          a.rvrs_dt                                                             as rvrs_dt,
          A.cr_rmng_amt                                                         as cr_rmng_amt,
          A.fncl_trnsct_type_name                                               as fncl_trnsct_type_name,
          A.cr_debit_bal_ind                                                    as cr_debit_bal_ind,
          A.pymt_adj_ind                                                        as pymt_adj_ind,
          A.dbl_cr_flag                                                         as dbl_cr_flag,
          A.dt_foto                                                             as dt_foto,
          b.segmento                                                            as segmento,  --SEGMENTO
          b.carteira                                                            as carteira, --CARTEIRA
          b.state_cd                                                            as state_cd,
          b.cycle_cd                                                            as cycle_cd,
          b.produto_subscr                                                      as produto_subscr

from h_garantiareceita_db.ATLYS_FAT_SP17_AJUSTE a

     --LEFT JOIN h_bigd_db.ATLYS_CLI_AGRUP_SP17 b (N O TEMOS INFO POR sbscrp_id
          --ON a.acct_nbr = b.acct_nbr
          --AND a.sbscrp_id = b.sbscrp_id
     
     LEFT JOIN (select acct_nbr                                     as acct_nbr,
                       --sbscrp_id                                    as sbscrp_id,
                       max(segmento)                                as segmento,
                       max(carteira)                                as carteira,
                       max(state_cd)                                as state_cd,
                       max(cycle_cd)                                as cycle_cd,
                       max(produto_subscr)                          as produto_subscr
                from h_garantiareceita_db.TBGD_MIRAI_ATLYS_CLI_AGRUP_CARGAS_CONTAS
                group by acct_nbr--, sbscrp_id
                ) b --CLIENTE
          ON a.acct_nbr = b.acct_nbr
          --AND a.sbscrp_id = b.sbscrp_id

--where c.tipo_cliente IS NULL
--limit 5
;


-----------------------------------------------------------------------------------------------------------------
--LIMPEZA
-----------------------------------------------------------------------------------------------------------------
DROP TABLE h_garantiareceita_db.ATLYS_FAT_SP17_AJUSTE;
-----------------------------------------------------------------------------------------------------------------
--FIM LIMPEZA
-----------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------
--CONTESTACAO -- POWERBI
-----------------------------------------------------------------------------------------------------------------
/* Campo	Tipo	Detalhe
alarme	Alarmes Normais	Código + Nome do alarme
Periodo	Alarmes Normais	Colocar em padrão AAAAMM
ciclo  	Alarmes Normais	Quando não tem "SEM CICLO"
regiao  	Alarmes Normais	Quando não tem "SEM UF"
segmento	Alarmes Normais	Quando não tem "SEM INFO"
carteira	Alarmes Normais	Quando não tem "SEM INFO"
tipo_produto (produto subscriber)	Alarmes Normais	Quando não tem "Faturamento - Outros"
sistema  	Alarmes Normais	Sistema VIVO - Kenan, Atlys, Next, etc
valor  	Alarmes Normais	Valor relacionado ao alarme - Por exemplo, se for um alarme relacionado a faturas - aqui temos que colocar a quantidade de faturas
qtd_contas  	Alarmes Normais	Qtdes de Contas
item_alarme	Alarmes Normais	Item que vai no alarme (Conforme solicitação VIVO - Pode ser numero total somente)

*/

INSERT OVERWRITE TABLE h_garantiareceita_db.tbgd_mirai_anlt_atlys PARTITION (alarme)

select a.adj_date                                                  as Periodo,
       NVL(a.cycle_cd,'SEM CICLO')                                 as Ciclo,
       NVL(a.state_cd,'SEM UF')                                    as Regiao,
       NVL(a.segmento,'B2B')                                       as Segmento,
       NVL(a.carteira,'SEM INFO')                                  as Carteira,
       NVL(a.produto_subscr,'CONTROLE B2B')                        as Tipo_produto,
       'ATLYS'                                                     as Sistema,
       concat(adj_cr_debit_ind , ' - ', adj_custom_bill_desc)      as Item_alarme,
       sum(adj_amt)                                                as Valor,
       count(*)                                                    as Qtd_Contas,    --distinct acct_nbr
       
       ''                                                          as flag_prorata, --'pro-rata', 'sem pro-rata', 'completo'
       ''                                                          as flag_ajuste,
       ''                                                          as flag_credito,
       ''                                                          as flag_taxa,
       ''                                                          as flag_desconto,
       ''                                                          as tipo_fat,
       ''                                                          as tipo_indicador,
       0                                                           as arpu,
       'AC-0001 - AJUSTES - CONTESTACOES'                          as Alarme --CAMPO > "ASSUNTO" DO ALARME
from   h_garantiareceita_db.tbgd_mirai_atlys_anlt_ajust_contest a
group by a.adj_date,
         NVL(a.cycle_cd,'SEM CICLO'),
         NVL(a.state_cd,'SEM UF'),
         NVL(a.segmento,'B2B'),
         NVL(a.carteira,'SEM INFO'),
         NVL(a.produto_subscr,'CONTROLE B2B'),
         'ATLYS',
         concat(adj_cr_debit_ind , ' - ', adj_custom_bill_desc),
         '',
         '',
         '',
         '',
         '',
         '',
         '',
         0,
         'AC-0001 - AJUSTES - CONTESTACOES'
--limit 5
;

-----------------------------------------------------------------------------------------------------------------
--FIM CONTESTACAO -- POWERBI
-----------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------
--CONSULTA DE DADOS - PIVOT
-----------------------------------------------------------------------------------------------------------------

--PIVOT CR------------------------------------------------------------------------------------
DROP TABLE IF EXISTS h_garantiareceita_db.tbgd_atlys_mirai_contestacao_CR_PIVOT; --h_garantiareceita_db.tbgd_ATLYS_mirai_FAT_ZERADO_PIVOT

CREATE TABLE IF NOT EXISTS h_garantiareceita_db.tbgd_atlys_mirai_contestacao_CR_PIVOT 
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY', 'transactional'='false')
AS

select adj_cr_debit_ind                                                          as adj_cr_debit_ind,
       NVL(segmento,'B2B')                                                       as segmento,
       NVL(produto_subscr,'CONTROLE B2B')                                        as produto_subscr,
       adj_custom_bill_desc                                                      as adj_custom_bill_desc,
       SUM(CASE WHEN adj_date = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -1), 'yyyyMM') THEN adj_amt ELSE 0 END) AS valor,
       SUM(CASE WHEN adj_date = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -2), 'yyyyMM') THEN adj_amt ELSE 0 END) AS valor_mes_ant_1,
       SUM(CASE WHEN adj_date = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -3), 'yyyyMM') THEN adj_amt ELSE 0 END) AS valor_mes_ant_2,
       SUM(CASE WHEN adj_date = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -4), 'yyyyMM') THEN adj_amt ELSE 0 END) AS valor_mes_ant_3,
       SUM(CASE WHEN adj_date = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -5), 'yyyyMM') THEN adj_amt ELSE 0 END) AS valor_mes_ant_4,
       SUM(CASE WHEN adj_date = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -6), 'yyyyMM') THEN adj_amt ELSE 0 END) AS valor_mes_ant_5,
       SUM(CASE WHEN adj_date = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -7), 'yyyyMM') THEN adj_amt ELSE 0 END) AS valor_mes_ant_6
FROM ( select adj_date                                                       as adj_date,
              adj_cr_debit_ind                                               as adj_cr_debit_ind,
              NVL(a.segmento,'B2B')                                          as segmento,
              NVL(produto_subscr,'CONTROLE B2B')                             as produto_subscr,
              adj_custom_bill_desc                                           as adj_custom_bill_desc,
              sum(adj_amt)                                                   as adj_amt
        from h_garantiareceita_db.tbgd_mirai_atlys_anlt_ajust_contest a
        where adj_cr_debit_ind = 'CR'
        group by adj_date,
                 adj_cr_debit_ind,
                 NVL(a.segmento,'B2B'), --verificar se é esta info mesmo
                 NVL(produto_subscr,'CONTROLE B2B'), --verificar se é esta info mesmo
                 adj_custom_bill_desc) b
GROUP BY adj_cr_debit_ind,
         segmento,
         produto_subscr,
         adj_custom_bill_desc
         
;

--PIVOT DB------------------------------------------------------------------------------------
DROP TABLE IF EXISTS h_garantiareceita_db.tbgd_atlys_mirai_contestacao_DB_PIVOT; --h_garantiareceita_db.tbgd_ATLYS_mirai_FAT_ZERADO_PIVOT

CREATE TABLE IF NOT EXISTS h_garantiareceita_db.tbgd_atlys_mirai_contestacao_DB_PIVOT 
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY', 'transactional'='false')
AS

select adj_cr_debit_ind                                                          as adj_cr_debit_ind,
       NVL(segmento,'B2B')                                                       as segmento,
       NVL(produto_subscr,'CONTROLE B2B')                                        as produto_subscr,
       adj_custom_bill_desc                                                      as adj_custom_bill_desc,
       SUM(CASE WHEN adj_date = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -1), 'yyyyMM') THEN adj_amt ELSE 0 END) AS valor,
       SUM(CASE WHEN adj_date = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -2), 'yyyyMM') THEN adj_amt ELSE 0 END) AS valor_mes_ant_1,
       SUM(CASE WHEN adj_date = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -3), 'yyyyMM') THEN adj_amt ELSE 0 END) AS valor_mes_ant_2,
       SUM(CASE WHEN adj_date = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -4), 'yyyyMM') THEN adj_amt ELSE 0 END) AS valor_mes_ant_3,
       SUM(CASE WHEN adj_date = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -5), 'yyyyMM') THEN adj_amt ELSE 0 END) AS valor_mes_ant_4,
       SUM(CASE WHEN adj_date = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -6), 'yyyyMM') THEN adj_amt ELSE 0 END) AS valor_mes_ant_5,
       SUM(CASE WHEN adj_date = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -7), 'yyyyMM') THEN adj_amt ELSE 0 END) AS valor_mes_ant_6
FROM ( select adj_date                                                       as adj_date,
              adj_cr_debit_ind                                               as adj_cr_debit_ind,
              NVL(a.segmento,'B2B')                                          as segmento,
              NVL(produto_subscr,'CONTROLE B2B')                             as produto_subscr,
              adj_custom_bill_desc                                           as adj_custom_bill_desc,
              sum(adj_amt)                                                   as adj_amt
        from h_garantiareceita_db.tbgd_mirai_atlys_anlt_ajust_contest a
        where adj_cr_debit_ind = 'DB'
        group by adj_date,
                 adj_cr_debit_ind,
                 NVL(a.segmento,'B2B'), --verificar se é esta info mesmo
                 NVL(produto_subscr,'CONTROLE B2B'), --verificar se é esta info mesmo
                 adj_custom_bill_desc) b
GROUP BY adj_cr_debit_ind,
         segmento,
         produto_subscr,
         adj_custom_bill_desc
         
;

--PIVOT FINAL------------------------------------------------------------------------------------
DROP TABLE IF EXISTS h_garantiareceita_db.tbgd_atlys_mirai_contestacao_FINAL1_PIVOT; 

CREATE TABLE IF NOT EXISTS h_garantiareceita_db.tbgd_atlys_mirai_contestacao_FINAL1_PIVOT 
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY', 'transactional'='false')
AS

SELECT adj_cr_debit_ind,
       segmento,
       produto_subscr,
       concat(adj_cr_debit_ind , ' - ', adj_custom_bill_desc) as ajust_desc,
       CASE 
           WHEN SUBSTR(adj_custom_bill_desc,1,1) IN ('0','1','2','3') AND LOWER(adj_custom_bill_desc) like '%jros%' THEN 'AJUSTE - JUROS'
           WHEN SUBSTR(adj_custom_bill_desc,1,1) IN ('0','1','2','3') AND LOWER(adj_custom_bill_desc) like '%multa%' THEN 'AJUSTE - MULTA'
           WHEN LOWER(adj_custom_bill_desc) like 'ajuste relativo a refatura x%' THEN 'AJUSTE - REFATURA'
           ELSE adj_custom_bill_desc
       END AS adj_custom_bill_desc,
       valor,
       valor_mes_ant_1,
       valor_mes_ant_2,
       valor_mes_ant_3,
       valor_mes_ant_4,
       valor_mes_ant_5,
       valor_mes_ant_6
FROM h_garantiareceita_db.tbgd_atlys_mirai_contestacao_CR_PIVOT

UNION ALL

SELECT adj_cr_debit_ind,
       segmento,
       produto_subscr,
       concat(adj_cr_debit_ind , ' - ', adj_custom_bill_desc) as ajust_desc,
       CASE 
           WHEN SUBSTR(adj_custom_bill_desc,1,1) IN ('0','1','2','3') AND LOWER(adj_custom_bill_desc) like '%jros%' THEN 'AJUSTE - JUROS'
           WHEN SUBSTR(adj_custom_bill_desc,1,1) IN ('0','1','2','3') AND LOWER(adj_custom_bill_desc) like '%multa%' THEN 'AJUSTE - MULTA'
           WHEN LOWER(adj_custom_bill_desc) like 'ajuste relativo a refatura x%' THEN 'AJUSTE - REFATURA'
           ELSE adj_custom_bill_desc
       END AS adj_custom_bill_desc,
       valor,
       valor_mes_ant_1,
       valor_mes_ant_2,
       valor_mes_ant_3,
       valor_mes_ant_4,
       valor_mes_ant_5,
       valor_mes_ant_6
FROM h_garantiareceita_db.tbgd_atlys_mirai_contestacao_DB_PIVOT

;



--PIVOT FINAL------------------------------------------------------------------------------------
DROP TABLE IF EXISTS h_garantiareceita_db.tbgd_atlys_mirai_contestacao_FINAL_PIVOT; 

CREATE TABLE IF NOT EXISTS h_garantiareceita_db.tbgd_atlys_mirai_contestacao_FINAL_PIVOT 
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY', 'transactional'='false')
AS

SELECT adj_cr_debit_ind,
       segmento,
       produto_subscr,
       concat(adj_cr_debit_ind , ' - ', adj_custom_bill_desc) as ajust_desc,
       valor,
       valor_mes_ant_1,
       valor_mes_ant_2,
       valor_mes_ant_3,
       valor_mes_ant_4,
       valor_mes_ant_5,
       valor_mes_ant_6
FROM h_garantiareceita_db.tbgd_atlys_mirai_contestacao_FINAL1_PIVOT

;

-----------------------------------------------------------------------------------------------------------------
--CONSULTA DE DADOS - PIVOT
-----------------------------------------------------------------------------------------------------------------

insert into h_garantiareceita_db.tbgd_mirai_alrm_atlys

select *
from h_garantiareceita_db.tbgd_mirai_alrm_atlys_bkp
where periodo = '202503'
;


select periodo, assunto, count(*)
from h_garantiareceita_db.tbgd_mirai_alrm_atlys_bkp
where periodo = '202503'
group by periodo, assunto
;

select periodo, assunto, count(*)
from h_garantiareceita_db.tbgd_mirai_alrm_atlys
--limit 5
group by periodo, assunto
;


-------------------------------------------------------------------------------------------------------------------------------------------------
--TABELA FNL - ALARME
--------------------------------------------------------------------------------------------------------------------------------------------------

insert into h_garantiareceita_db.tbgd_mirai_alrm_atlys

select              
        f.segmento                                                                                                                     as segmento,
        'SEM INFO'                                                                                                                      as tipo_cliente,
        f.ajust_desc                                                                                                                      as item_alarme, 
        'PERIODO'                                                                                                                       as produto, 
        'SEM CLASSIFICACAO'                                                                                                             as tipo_fat,
        DATE_FORMAT(add_months(current_date,-1),'YYYYMM')                                                                               as periodo,
        CAST(f.valor AS DECIMAL(38, 2))                                                                                                 as valor,
        ROUND(CASE WHEN valor = 0 OR valor is null then 0
            ELSE ABS((valor/m.media)-1) END,2)                                                                                          as media_valor, 
        '0'                                                                                                                             as media_quant,
        '0'                                                                                                                             as dif_media_valor_x_quant, 
        CAST(f.valor / f.valor_mes_ant_1 AS DECIMAL(38, 2))                                                                             as var_mes_ant_1,
        CAST(valor_mes_ant_1 AS DECIMAL(38, 2))                                                                                         as valor_mes_ant_1,
        CAST(f.valor_mes_ant_1 / f.valor_mes_ant_2 AS DECIMAL(38, 2))                                                                   as var_mes_ant_2,
        CAST(valor_mes_ant_2 AS DECIMAL(38, 2))                                                                                         as valor_mes_ant_2,
        CAST(f.valor_mes_ant_2 / f.valor_mes_ant_3 AS DECIMAL(38, 2))                                                                   as var_mes_ant_3,
        CAST(valor_mes_ant_3 AS DECIMAL(38, 2))                                                                                         as valor_mes_ant_3,
        CAST(f.valor_mes_ant_3 / f.valor_mes_ant_4 AS DECIMAL(38, 2))                                                                   as var_mes_ant_4,
        CAST(valor_mes_ant_4 AS DECIMAL(38, 2))                                                                                         as valor_mes_ant_4,
        CAST(f.valor_mes_ant_4 / f.valor_mes_ant_5 AS DECIMAL(38, 2))                                                                   as var_mes_ant_5,
        CAST(valor_mes_ant_5 AS DECIMAL(38, 2))                                                                                         as valor_mes_ant_5,
        CAST(f.valor_mes_ant_5 / f.valor_mes_ant_6 AS DECIMAL(38, 2))                                                                   as var_mes_ant_6,
        CAST(valor_mes_ant_6 AS DECIMAL(38, 2))                                                                                         as valor_mes_ant_6,
        CAST((
                CAST(f.valor / f.valor_mes_ant_1 AS DECIMAL(38, 2)) +
                CAST(f.valor_mes_ant_1 / f.valor_mes_ant_2 AS DECIMAL(38, 2)) +
                CAST(f.valor_mes_ant_2 / f.valor_mes_ant_3 AS DECIMAL(38, 2)) +
                CAST(f.valor_mes_ant_3 / f.valor_mes_ant_4 AS DECIMAL(38, 2)) +
                CAST(f.valor_mes_ant_4 / f.valor_mes_ant_5 AS DECIMAL(38, 2)) +
                CAST(f.valor_mes_ant_5 / f.valor_mes_ant_6 AS DECIMAL(38, 2))
            ) / 6 AS DECIMAL(38, 2))                                                                                                    as med_var_mes,
        r.cod                                                                                                                           as cod,
        r.tipo                                                                                                                          as tipo,
        r.assunto                                                                                                                       as assunto,
        'n'                                                                                                                             as redutor,
        r.sistema                                                                                                                       as sistema,
        r.etapa                                                                                                                         as etapa,
        m.media                                                                                                                         as media_meses,
        CASE                                       
            WHEN m.media >= 0 THEN 
                ROUND(m.media - (m.media * fator_perc_inf), 2) 
            ELSE 
                ROUND(m.media + (ABS(m.media) * fator_perc_inf), 2) 
        END            
                                                                                                                                       as limite_inferior,
        CASE 
            WHEN m.media >= 0 THEN 
                ROUND(m.media + (m.media * fator_perc_sup), 2) 
            ELSE 
                ROUND(m.media - (ABS(m.media) * fator_perc_sup), 2) 
        END                                                                                                                             as limite_superior,
        '0'                                                                                                                             as imp_x_fat_atual,
        '0'                                                                                                                             as imp_x_fat_media,
        r.fator_perc_inf                                                                                                                as fator_perc_inf, 
        r.fator_perc_sup                                                                                                                as fator_perc_sup, 
        r.fator_num_inf                                                                                                                 as fator_num_inf,
        r.fator_num_sup                                                                                                                 as fator_num_sup,
        r.fator_perc_composto                                                                                                           as fator_perc_composto,
        case
               when valor > CAST((m.media+(m.media*fator_perc_sup)+fator_num_sup) AS DECIMAL(38, 2)) then 's'
            --   when valor < CAST((m.media-ABS((m.media*fator_perc_inf)+fator_num_inf)) AS DECIMAL(38, 2)) then 's'
               else 'n'
        end                                                                                                                              as flag_alarme,
        case 
               when valor > CAST((m.media+(m.media*fator_perc_sup)+fator_num_sup) AS DECIMAL(38, 2)) then 'Ultrapassou Limite Superior' 
             --  when valor < CAST((m.media-ABS((m.media*fator_perc_inf)+fator_num_inf)) AS DECIMAL(38, 2)) then 'Ultrapassou Limite Inferior'
               else 'Normal'
        end                                                                                                                              as alarme,
        r.ponto                                                                                                                          as ponto,
        'valor ajuste'                                                                                                                   as referencia_valor,
        r.rel_ind_produto                                                                                                                as rel_ind_produto,
        'SEM CLASS'                                                                                                                      as tecnologia,
        current_date()                                                                                                                   as data_alarme,
        '0'                                                                                                                              as quant,
        '0'                                                                                                                              as quant_mes_ant,
        '0'                                                                                                                              as quant_med,
        '0'                                                                                                                              as limite_inferior_quant,
        '0'                                                                                                                              as limite_superior_quant,
        '0'                                                                                                                              as fator_perc_inf_quant,
        '0'                                                                                                                              as fator_perc_sup_quant,
        '0'                                                                                                                              as arpu,
        '0'                                                                                                                              as arpu_mes_ant,
        'MENSAL'                                                                                                                         as periodicidade
from h_garantiareceita_db.tbgd_atlys_mirai_contestacao_FINAL_PIVOT f
     JOIN (
            SELECT segmento,
                   adj_cr_debit_ind,
                   produto_subscr,
                   ajust_desc,
                    CAST(SUM(valor_mes_ant_1 + valor_mes_ant_2 + valor_mes_ant_3 + valor_mes_ant_4 + valor_mes_ant_5 + valor_mes_ant_6) / 6 AS DECIMAL(38, 2)) AS media
            FROM h_garantiareceita_db.tbgd_atlys_mirai_contestacao_FINAL_PIVOT
            GROUP BY segmento,
                     adj_cr_debit_ind,
                     produto_subscr,
                     ajust_desc
                     
         ) m
         ON m.segmento = f.segmento 
         and m.adj_cr_debit_ind = f.adj_cr_debit_ind 
         and m.produto_subscr = f.produto_subscr
         and m.ajust_desc = f.ajust_desc
    INNER JOIN h_garantiareceita_db.tbgd_mirai_fator_atlys r 
        ON r.tipo_cliente = f.segmento
        AND r.assunto = 'AC-0001 - AJUSTES - CONTESTACOES' 
        AND r.sistema = 'ATLYS'
        AND r.chave = f.ajust_desc
        and r.rel_ind_produto = f.produto_subscr
        AND r.cod between 'PAT5000001' and 'PAT5010000' --CODIGO PEGAR COM EVERTON
        --limit 50
        ;

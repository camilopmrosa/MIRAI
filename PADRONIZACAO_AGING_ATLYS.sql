-- ***********************************************************************************************************************************
-- Nome do Projeto   : Mirai
-- Alarme            : 
-- Descrição         : AGING   Avaliação do tempo de espera entre a Data de Vencimento da fatura x Data de Pagamento. 
--                   :          Por exemplo, se a fatura foi disponibilizada em 23/09 e paga em 25/09, o AGING seria de 2 dias. O cálculo mensal consiste na média do AGING de todas as faturas pagas pelos clientes dentro do mês em questão.
-- Autor             : Everton Itamar Werka Pereira
-- Data de Criação   : 14/04/2025
-- Última Modificação: -
-- Versão            : 1.0
-- Observações       : Tabela Analitica: h_bigd_db.TBGD_MIRAI_ATLYS_AGING_FINAL_TESTE_SP15_PADR / Tabela Alarme Final: h_bigd_db.tbgd_mirai_alrm_fnl_atlys_segmento
-- Ferramenta        : Hive / Shell
-- ***********************************************************************************************************************************
-- Histórico de Modificações:
-- Data                    Alteração                        Autor
-- [Data da Modificação]  [Descrição da Modificação]       [Nome do Autor da Modificação]
-- ***********************************************************************************************************************************

--CONFIGURACAO PADRÃO VIVO (SOLICITADO)--------------------------------------------------------------------------
SET hive.merge.tezfiles=true;
SET hive.merge.mapfiles = true;
SET hive.merge.mapredfiles = true;
SET hive.merge.size.per.task = 134217728;
SET hive.merge.smallfiles.avgsize = 134217728;
SET hive.exec.compress.output = true;
--FIM CONFIGURACAO PADRÃO VIVO (SOLICITADO)----------------------------------------------------------------------



-----------------------------------------------------------------------------------------------------------------
--AGING - Tempo de pagamento de faturas
-----------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------
--CARGA CONTAS
-----------------------------------------------------------------------------------------------------------------
DROP TABLE h_garantiareceita_db.tbgd_MIRAI_ATLYS_CLI_AGRUP_CARGAS_CONTAS_SP15_AGING_ATLYS; --h_garantiareceita_db.

--CREATE TEMPORARY TABLE IF NOT EXISTS h_bigd_db.tbgd_MIRAI_ATLYS_CLI_AGRUP_CARGAS_CONTAS_PADR
CREATE TABLE IF NOT EXISTS h_garantiareceita_db.tbgd_MIRAI_ATLYS_CLI_AGRUP_CARGAS_CONTAS_SP15_AGING_ATLYS
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY', 'transactional'='false')
AS

select a.acct_nbr                                                   as acct_nbr, --equivale a account_no
       a.sbscrp_id                                                  as sbscrp_id, --NOVO
       max(nvl(c.segmento, 'SEM INFO'))                             as carteira,
       max(a.state_cd)                                              as state_cd, --equivale a cust_state
       max(a.cust_idntfc_type_cd)                                   as cust_idntfc_type_cd,
       max(a.cust_id_text)                                          as cust_id_text,
       max(a.cycle_cd)                                              as cycle_cd,
       max(a.acct_status_id)                                        as acct_status_id,
       max(CASE WHEN a.cust_type_group_cd IN ('GCN','GOV','PME') THEN 'B2B'
            ELSE 'B2C'
       END)                                                         as segmento, --tipo_cliente igual
       max(a.cust_type_group_cd)                                    as cust_type_group_cd,
       max(a.cust_type_cd)                                          as cust_type_cd, --NOVO
       max(a.cust_type_desc)                                        as cust_type_desc,
       
       
       --max(f.produto)                                               as produto_subscr, --NOVO PARA TER PRODUTO
       
       max(CASE WHEN f.sbscrp_type_desc = 'INTERNET FWT'  THEN (CASE WHEN a.cust_idntfc_type_cd = 'CNPJ' THEN 'FWT B2C' ELSE 'FWT B2B' END)
            ELSE NVL(f.produto,'Faturamento - Outros')
       END)                                                          as produto_subscr,
       
       max(a.sbscrp_type_cd)                                        as sbscrp_type_cd, --NOVO
       max(b.sbscrp_type_desc)                                      as sbscrp_type_desc, --NOVO
       max(b.mbl_fixed_ind)                                         as mbl_fixed_ind,
       max(a.access_nbr)                                            as access_nbr,

       count(*) as qtde

from (select distinct * from p_bigd_camada_semantica_db.tbgd_tfat_mvel_agrp_clnt_atlys) a

          JOIN (select x.sbscrp_id as sbscrp_id, 
                       max(x.sbscrp_asgm_eff_dt) as sbscrp_asgm_eff_dt--,
                       --max(x.sbscrp_svc_eff_dt) as sbscrp_svc_eff_dt--,
                       --max(x.acct_status_eff_dt) as acct_status_eff_dt,
                       --max(x.acct_status_eff_tm) as acct_status_eff_tm
                from p_bigd_camada_semantica_db.tbgd_tfat_mvel_agrp_clnt_atlys x
                where x.prim_cust_idntfc_flag = 'Y'
                --and x.sbscrp_id = '0437762119'
                group by x.sbscrp_id
                ) x
                ON a.sbscrp_id = x.sbscrp_id
                AND a.sbscrp_asgm_eff_dt = x.sbscrp_asgm_eff_dt
                --AND a.sbscrp_svc_eff_dt = c.sbscrp_svc_eff_dt
                --and a.acct_status_eff_dt = c.acct_status_eff_dt
                --and a.acct_status_eff_tm = c.acct_status_eff_tm

          LEFT JOIN (select * from p_bigd_db.tbgd_tatlys_sbscrp_type where dt_foto = (select max(dt_foto) as dt_foto from p_bigd_db.tbgd_tatlys_sbscrp_type)) b
            --LEFT JOIN (select * from p_bigd_db.tbgd_tatlys_sbscrp_type where dt_foto = '20250317') b
               ON a.sbscrp_type_cd = b.sbscrp_type_cd
               
          LEFT JOIN h_garantiareceita_db.tbgdt_subscrp_type_atlys f
               ON a.sbscrp_type_cd = f.sbscrp_type_cd
          
          LEFT JOIN (select documento, max(segmento) as segmento from p_garantiareceita_db.acn_dbf_subsegmentos_bau group by documento) c
             on a.cust_id_text = c.documento
             
where a.prim_cust_idntfc_flag = 'Y'
--AND a.cust_type_group_cd IN ('GCN','GOV','PME') --retirado conforme orientação
group by a.acct_nbr, a.sbscrp_id
--limit 5
;

-----------------------------------------------------------------------------------------------------------------
--FIM CARGA CONTAS
-----------------------------------------------------------------------------------------------------------------

/* RETIRAR
--CARGA CONTAS/CLIENTES--------------------------------------------------------------------------------------------------------
--DADOS DOS CLIENTES NO ATLYS--------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS h_bigd_db.ATLYS_CLI_AGRUP_AGING_TESTE_SP15_PADR; --NOME EXEMPLO (ALTERAR NA VERSÃO OFICIAL)
CREATE TEMPORARY TABLE IF NOT EXISTS h_bigd_db.ATLYS_CLI_AGRUP_AGING_TESTE_SP15_PADR --NOME EXEMPLO (ALTERAR NA VERSÃO OFICIAL)
AS

select a.acct_nbr                                                   as acct_nbr, --equivale a account_no
       max(nvl(c.segmento, 'SEM INFO'))                             as tipo_segmento_bau,
       max(a.state_cd)                                              as state_cd, --equivale a cust_state
       max(a.cust_idntfc_type_cd)                                   as cust_idntfc_type_cd,
       max(a.cust_id_text)                                          as cust_id_text,
       max(a.cycle_cd)                                              as cycle_cd,
       max(a.acct_status_id)                                        as acct_status_id,
       max(CASE WHEN a.cust_type_group_cd IN ('GCN','GOV','PME') THEN 'B2B'
            ELSE 'B2C'
       END)                                                         as tipo_cliente, --tipo_cliente igual

       count(*) as qtde
from p_bigd_camada_semantica_db.tbgd_tfat_mvel_agrp_clnt_atlys a

        left join (select documento, max(segmento) as segmento from p_garantiareceita_db.acn_dbf_subsegmentos_bau group by documento) c
        on a.cust_id_text = c.documento

where a.prim_cust_idntfc_flag = 'Y'
group by a.acct_nbr
;
*/

-----------------------------------------------------------------------------------------------------------------
--CARGA AGING
-----------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS h_garantiareceita_db.TBGD_MIRAI_ATLYS_AGING_SP15;

CREATE TEMPORARY  TABLE IF NOT EXISTS h_garantiareceita_db.TBGD_MIRAI_ATLYS_AGING_SP15
AS
SELECT DISTINCT
    SUBSTR(a.last_bill_dt, 1, 6)                                                                AS last_bill_dt,
    a.last_bill_dt                                                                              AS data_faturamento,
    a.acct_nbr                                                                                  AS acct_nbr,
    CASE
        WHEN hold_bill_ind = 'B' THEN 'B - Fatura retida por múltiplas cópias'
        WHEN hold_bill_ind = 'H' THEN 'H - Retido por algum outro motivo'
        ELSE 'N - Normal'
    END                                                                                         AS hold_bill_ind,
    hold_bill_ind                                                                               AS hold_bill_ind_cod,
    SUBSTR(a.held_bill_reason_id, 1, 2)                                                         AS held_bill_reason_id,
    CASE
        WHEN a.last_bill_pymt_amt <> 0 THEN 'Pagamento Duplicado'
        WHEN a.last_bill_adj_amt <> 0 THEN 'Ajustes'
        ELSE 'Normal'
    END                                                                                         AS status_problemas,
    a.last_bill_pymt_amt                                                                        AS last_bill_pymt_amt,
    a.last_bill_adj_amt                                                                         AS last_bill_adj_amt,
    b.held_bill_reason_desc                                                                     AS held_bill_reason_desc,
    a.bill_amt                                                                                  AS bill_amt,
    a.bill_pymt_due_dt                                                                          AS dt_vencimento,
    a.bill_pymt_past_due_dt                                                                     AS dt_pagamento,
    a.paid_amt                                                                                  AS valor_pago,
    -- Cálculo de aging (diferença entre pagamento e vencimento, ajustado 0 para valores NULL)
    COALESCE(CASE
    WHEN DATEDIFF(from_unixtime(unix_timestamp(bill_pymt_past_due_dt, 'yyyyMMdd'), 'yyyy-MM-dd'),from_unixtime(unix_timestamp(bill_pymt_due_dt, 'yyyyMMdd'), 'yyyy-MM-dd')) = 0 THEN 0
    ELSE DATEDIFF(from_unixtime(unix_timestamp(bill_pymt_past_due_dt, 'yyyyMMdd'), 'yyyy-MM-dd'),from_unixtime(unix_timestamp(bill_pymt_due_dt, 'yyyyMMdd'), 'yyyy-MM-dd'))
END, 0)                                                                                     AS aging
FROM
    -- Filtro inicial na tabela principal
    (SELECT *
     FROM p_bigd_db.tbgdt_atlys_bill
     WHERE dt_foto = (SELECT MAX(dt_foto) FROM p_bigd_db.tbgdt_atlys_bill)
    ) a
LEFT JOIN
    -- Subquery para held_bill_reason
    (
        SELECT
            held_bill_reason_cd,
            MAX(held_bill_reason_desc) AS held_bill_reason_desc
        FROM
            p_bigd_db.tbgd_tatlys_held_bill_reason
        WHERE
            dt_foto = (SELECT MAX(dt_foto) FROM p_bigd_db.tbgd_tatlys_held_bill_reason)
        GROUP BY
            held_bill_reason_cd
    ) b
    ON SUBSTR(a.held_bill_reason_id, 1, 2) = b.held_bill_reason_cd
/*
LEFT JOIN
    -- Subquery para bsv_pymt
    (
        SELECT
            acct_nbr
        FROM
            p_bigd_db.tbgdt_atlys_bsv_pymt
        WHERE
            dt_foto = (SELECT MAX(dt_foto) FROM p_bigd_db.tbgdt_atlys_bsv_pymt)
        GROUP BY
            acct_nbr
    ) c
    ON a.acct_nbr = c.acct_nbr
*/
WHERE SUBSTR(a.last_bill_dt, 1, 6) >= DATE_FORMAT(add_months(current_date, -7), 'YYYYMM')
    AND SUBSTR(a.last_bill_dt, 1, 6) <= DATE_FORMAT(add_months(current_date, -1), 'YYYYMM')
    and a.bill_amt = a.paid_amt --SOMENTE PAGAMENTOS COMPLETOS > SE TIVER QUE ALTERAR PRA TUDO, USAR UMA REGRA ONDE a.paid_amt > 0
;
-----------------------------------------------------------------------------------------------------------------
--FIM CARGA AGING
-----------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------
--CARGA AGING x CLIENTE
-----------------------------------------------------------------------------------------------------------------
DROP TABLE IF EXISTS h_garantiareceita_db.tbgd_mirai_atlys_anlt_aging_fat; --tbgd_mirai_atlys_anlt_fat_zerada

CREATE TABLE IF NOT EXISTS h_garantiareceita_db.tbgd_mirai_atlys_anlt_aging_fat
AS
select  a.last_bill_dt                                           as last_bill_dt,
        'Faturamento - Outros'                                   as produto_subscr, --TIPO_PRODUTO
        a.data_faturamento                                       as data_faturamento,
        a.acct_nbr                                               as acct_nbr,
        a.hold_bill_ind                                          as hold_bill_ind,
        a.hold_bill_ind_cod                                      as hold_bill_ind_cod,
        a.held_bill_reason_id                                    as held_bill_reason_id,
        a.status_problemas                                       as status_problemas,
        a.last_bill_pymt_amt                                     as last_bill_pymt_amt,
        a.last_bill_adj_amt                                      as last_bill_adj_amt,
        a.held_bill_reason_desc                                  as held_bill_reason_desc,
        a.bill_amt                                               as bill_amt,
        a.dt_vencimento                                          as dt_vencimento,
        a.dt_pagamento                                           as dt_pagamento,
        a.valor_pago                                             as valor_pago,
        a.aging                                                  as aging,
        b.segmento                                               as segmento, --SEGMENTO
        b.carteira                                               as carteira, --CARTEIRA
        b.state_cd                                               as state_cd,
        b.cycle_cd                                               as cycle_cd
        
from h_garantiareceita_db.TBGD_MIRAI_ATLYS_AGING_SP15 a --AGING
     LEFT JOIN (select acct_nbr                                     as acct_nbr,
                       max(segmento)                                as segmento,
                       max(carteira)                                as carteira,
                       max(state_cd)                                as state_cd,
                       max(cycle_cd)                                as cycle_cd
                from h_garantiareceita_db.tbgd_MIRAI_ATLYS_CLI_AGRUP_CARGAS_CONTAS_SP15_AGING_ATLYS
                group by acct_nbr) b --CLIENTE
          ON a.acct_nbr = b.acct_nbr
;
-----------------------------------------------------------------------------------------------------------------
--FIM CARGA AGING x CLIENTE
-----------------------------------------------------------------------------------------------------------------

-----------------------------------------------------------------------------------------------------------------
--LIMPEZA
-----------------------------------------------------------------------------------------------------------------
DROP TABLE h_garantiareceita_db.tbgd_MIRAI_ATLYS_CLI_AGRUP_CARGAS_CONTAS_SP15_AGING_ATLYS;
DROP TABLE h_garantiareceita_db.TBGD_MIRAI_ATLYS_AGING_SP15;
-----------------------------------------------------------------------------------------------------------------
--FIM LIMPEZA
-----------------------------------------------------------------------------------------------------------------

















-----------------------------------------------------------------------------------------------------------------
--AGING x CLIENTE -- POWERBI
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

DROP TABLE h_garantiareceita_db.tbgd_mirai_anlt_atlys_aging_pagto_resumo;

CREATE  TABLE IF NOT EXISTS h_garantiareceita_db.tbgd_mirai_anlt_atlys_aging_pagto_resumo
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY','transactional'='true')
AS


select 'ID 21 - Aging de Pagamento de Fatura'                      as Alarme, --CAMPO > "ASSUNTO" DO ALARME
       a.last_bill_dt                                              as Periodo,
       a.cycle_cd                                                  as Ciclo,
       a.state_cd                                                  as Regiao,
       NVL(a.segmento,'B2B')                                       as Segmento,
       a.carteira                                                  as Carteira,
       NVL(a.produto_subscr,'CONTROLE B2B')                        as Tipo_produto,
       'ATLYS'                                                     as Sistema,
       'PERIODO'                                                   as Item_alarme,
       ROUND(AVG(aging), 2)                                        as Valor,
       count(distinct acct_nbr)                                    as Qtd_Contas    
from   h_garantiareceita_db.tbgd_mirai_atlys_anlt_aging_fat a
group by 'ID 21 - Aging de Pagamento de Fatura',
         a.last_bill_dt,
         a.cycle_cd,
         a.state_cd,
         a.segmento,
         a.carteira,
         a.produto_subscr,
         'ATLYS',
         'PERIODO'
--limit 5
;

-----------------------------------------------------------------------------------------------------------------
--AGING x CLIENTE -- POWERBI
-----------------------------------------------------------------------------------------------------------------






-----------------------------------------------------------------------------------------------------------------
--CONSULTA DE DADOS - PIVOT
-----------------------------------------------------------------------------------------------------------------

DROP TABLE h_garantiareceita_db.TBGD_ATLYS_MIRAI_AGING_PAGTO_PIVOT;

CREATE  TABLE IF NOT EXISTS h_garantiareceita_db.TBGD_ATLYS_MIRAI_AGING_PAGTO_PIVOT
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY','transactional'='true')
AS


SELECT
    NVL(b.segmento, 'B2B') AS segmento,
    b.produto_subscr AS produto_subscr,
    --b.hold_bill_ind AS hold_bill_ind,
    round(SUM(CASE WHEN b.last_bill_dt = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -1), 'yyyyMM') THEN qtde ELSE 0 END),2) AS valor,
    round(SUM(CASE WHEN b.last_bill_dt = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -2), 'yyyyMM') THEN qtde ELSE 0 END),2) AS valor_mes_ant_1,
    round(SUM(CASE WHEN b.last_bill_dt = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -3), 'yyyyMM') THEN qtde ELSE 0 END),2) AS valor_mes_ant_2,
    round(SUM(CASE WHEN b.last_bill_dt = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -4), 'yyyyMM') THEN qtde ELSE 0 END),2) AS valor_mes_ant_3,
    round(SUM(CASE WHEN b.last_bill_dt = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -5), 'yyyyMM') THEN qtde ELSE 0 END),2) AS valor_mes_ant_4,
    round(SUM(CASE WHEN b.last_bill_dt = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -6), 'yyyyMM') THEN qtde ELSE 0 END),2) AS valor_mes_ant_5,
    round(SUM(CASE WHEN b.last_bill_dt = DATE_FORMAT(ADD_MONTHS(CURRENT_DATE, -7), 'yyyyMM') THEN qtde ELSE 0 END),2) AS valor_mes_ant_6
FROM (
    SELECT
        last_bill_dt,
        --hold_bill_ind,
        segmento,
        produto_subscr,
        ROUND(AVG(aging), 2) AS qtde
    FROM h_garantiareceita_db.tbgd_mirai_atlys_anlt_aging_fat --TABELA ANALITICA PRINCIPAL
    GROUP BY last_bill_dt, hold_bill_ind, segmento, produto_subscr
) b
GROUP BY --b.hold_bill_ind, 
NVL(b.segmento, 'B2B'), 
b.produto_subscr 
;

-----------------------------------------------------------------------------------------------------------------
--FIM CONSULTA DE DADOS - PIVOT
-----------------------------------------------------------------------------------------------------------------


















--------------------------------------------------------------------------------------------------------------------------------------------------
--TABELA FNL - ALARME
--------------------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE h_garantiareceita_db.tbgd_mirai_alrm_atlys_aging_fat;

CREATE  TABLE IF NOT EXISTS h_garantiareceita_db.tbgd_mirai_alrm_atlys_aging_fat
STORED AS ORC
TBLPROPERTIES ('orc.compress'='SNAPPY','transactional'='true')
AS

--insert into h_bigd_db.tbgd_mirai_alrm_fnl_atlys_segmento

select
        f.segmento                                                                                                                      as segmento,
        'SEM INFO'                                                                                                                      as tipo_cliente,
        'PERIODO'                                                                                                                       as item_alarme,
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
        END                                                                                                                            as limite_inferior,
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
               when valor > CAST((m.media+(m.media*fator_perc_sup)) AS DECIMAL(38, 2)) then 's'
               when valor < CAST((m.media-ABS((m.media*fator_perc_inf))) AS DECIMAL(38, 2)) then 's'
               else 'n'
        end                                                                                                                           as flag_alarme,
        case
               when valor > CAST((m.media+(m.media*fator_perc_sup)) AS DECIMAL(38, 2)) then 'Ultrapassou Limite Superior'
               when valor < CAST((m.media-ABS((m.media*fator_perc_inf))) AS DECIMAL(38, 2)) then 'Ultrapassou Limite Inferior'
               else 'Normal'
        end                                                                                                                              as alarme,
        r.ponto                                                                                                                          as ponto,
        'Media Dias Atraso'                                                                                                                   as referencia_valor,
        r.rel_ind_produto                                                                                                                 as rel_ind_produto,
        'SEM CLASS'                                                                                                                      as tecnologia,
        current_timestamp()                                                                                                                   as data_alarme,
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
from h_garantiareceita_db.TBGD_ATLYS_MIRAI_AGING_PAGTO_PIVOT f
    JOIN (
            SELECT segmento,
                   produto_subscr,
                --    held_bill_reason_id,
                    CAST(SUM(valor_mes_ant_1 + valor_mes_ant_2 + valor_mes_ant_3 + valor_mes_ant_4 + valor_mes_ant_5 + valor_mes_ant_6) / 6 AS DECIMAL(38, 2)) AS media
            FROM h_garantiareceita_db.TBGD_ATLYS_MIRAI_AGING_PAGTO_PIVOT
            GROUP BY segmento,
                     produto_subscr
                    --,held_bill_reason_id
         ) m
         ON m.segmento = f.segmento
        --  and m.status = f.status
         and m.produto_subscr = f.produto_subscr
        --and m.held_bill_reason_id = f.held_bill_reason_id
    INNER JOIN h_garantiareceita_db.tbgd_mirai_fator_atlys r
        ON f.segmento = r.tipo_cliente
        -- AND r.assunto = f.status
        AND r.sistema = 'ATLYS'
        -- AND r.rel_ind_produto = f.produto_subscr
        -- AND SUBSTR('0'||r.chave,-2) = f.held_bill_reason_id --COLOCAR AND DE CODIGOS AQUI
        AND r.cod between 'PAT5020001' and 'PAT5030000'
;

-------------------------------------------------------------------

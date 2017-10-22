
declare @date11 datetime
declare @date22 datetime
declare @id_store int

declare @date1 datetime
declare @date2 datetime

--set @date11 = '26.08.2017'
--set @date22 = '27.08.2017'
--set @id_store = 1

set @date11 = '!datefirst!'
set @date22 = '!datelast!'
set @id_store =  !store!

SET @date1 = CONVERT(DATETIME, LEFT(CONVERT(VARCHAR(20), @date11, 121),10), 121)
SET @date2 = CONVERT(DATETIME, LEFT(CONVERT(VARCHAR(20), @date22, 121),10) + ' 23:59:59.997', 121)

select a.d_type+';'+a.n_dok+';'+a.d_dok+';'+a.supplier+';'+a.supplier_INN+';'+a.N_KKM+';'+a.N_Chek+';'+a.FIO_Chek+';'+a.Disk_T+';'+a.Disk_Sum+';'+a.Sum_Zak+';'+a.Sum_Rozn+';'+a.PP_Teg+';'+a.Drug_code+';'+a.Drug_name+';'+a.Drug_Producer_Code+';'+a.Drug_Producer_Name+';'+a.Drug_Producer_Country+';'+a.Drug_Bar+';'+a.Cena_Zak+';'+a.Cena_Rozn+';'+ replace(convert(varchar,a.Quant),'.',',')+';'+a.Serial+';'+a.Godn+';'+a.Barecode as data

from 
(
select 
 '0' d_type,
l.DOC_NUM as n_dok,
(select dbo.formatdatetime('ddmmyyyy',@date1)) as d_dok,
(select name from contractor where id_contractor=l.ID_SUPPLIER) as supplier, 
(select isnull (INN,' ')  from contractor where id_contractor=l.ID_SUPPLIER) as supplier_INN, 
'' as N_KKM,
'' as N_Chek,
'' as FIO_Chek,
'' as Disk_T,                                                                                                                           
'' as Disk_Sum,  
'' as Sum_Zak,
''  as Sum_Rozn,--replace(convert(varchar,l.PRICE_Sal*lm.QUANTITY_SUB),'.',',') as Sum_Rozn,
'0'as PP_Teg,
convert(varchar,l.ID_GOODS) as Drug_code,
(select  name from goods where id_goods=l.ID_GOODS) as Drug_name,
convert(varchar,(select id_producer from goods where id_goods=l.ID_GOODS)) as Drug_Producer_Code,
(select name from producer where id_producer=(select  id_producer from goods where id_goods=l.ID_GOODS)) as Drug_Producer_Name,
(select name from country where id_country=(select id_country from producer where id_producer=(select  id_producer from goods where id_goods=l.id_goods))) as Drug_Producer_Country,
isnull((select top(1) code from bar_code where id_goods=l.ID_GOODS),'1234567890123') as Drug_Bar,
replace(convert(varchar,l.PRICE_SUP),'.',',') as Cena_Zak,
replace(convert(varchar,l.PRICE_SAL),'.',',') as Cena_Rozn,

(SELECT 
	
	AMOUNT_OST = SUM(LM2.QUANTITY_ADD - LM2.QUANTITY_SUB)
FROM LOT_movement LM2
INNER JOIN LOT L2 ON L2.ID_LOT_GLOBAL = LM2.ID_LOT_GLOBAL
WHERE
LM2.ID_LOT_GLOBAL=l.id_lot_global  and 
(lm2.QUANTITY_ADD!=0 or lm2.QUANTITY_SUB!=0)   
and
LM2.DATE_OP <=@date1
GROUP BY lm2.ID_LOT_GLOBAL
)
as Quant,
isnull((select SERIES_NUMBER from SERIES where ID_SERIES=l.ID_SERIES),'12345') as Serial,
isnull((select dbo.formatdatetime('ddmmyyyy', BEST_BEFORE) from SERIES where ID_SERIES=l.ID_SERIES),'01012020') as Godn,
l.INTERNAL_BARCODE as Barecode,
LM.CODE_OP, lm.ID_TABLE
from lot_movement lm,lot l
where 
lm.ID_LOT_GLOBAL=l.id_lot_global

and 
lm.date_op<=@date1-- and convert(datetime,'01.04.2016 21:00')

and lm.op not in ('SUB','RES','CHEQUE_RES')
and l.id_store=@id_store
) a where a.Quant>0

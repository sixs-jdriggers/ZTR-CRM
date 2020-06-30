DECLARE @Company as varchar(5)
DECLARE @OrderDate as Date
DECLARE @OrderNum int

SET @Company = '2000'
SET @OrderDate = '2017-02-16'
SET @OrderNum = #ORDERNUM#

SELECT  OH.OrderNum,
		OH.PONum,
		OH.OrderDate,
		OD.OpenLine,
		OD.OrderLine,
		OD.PartNum,
		OD.RevisionNum,
		OD.LineDesc,
		OD.SellingQuantity,
		OD.SalesUM,
		ISNULL(OD.NeedByDate, '2000-01-01') as NeedByDate,
		ISNULL(OD.RequestDate, '2000-01-01') as RequestDate,
		C.CustID as CRMCustID,
		CASE CUD.CRMAccountID_c WHEN '' THEN 'FA244793-684A-E711-80CE-005056A4467D' ELSE CUD.CRMAccountID_c END as CRMGuid,
		OD.DocUnitPrice,
		DocExtPriceDtl,
		OH.DocOrderAmt,
		C.Name
FROM	Erp.OrderHed OH
INNER JOIN Erp.OrderDtl OD ON OD.Company = OH.Company and OD.OrderNum = OH.OrderNum
INNER JOIN Erp.Customer C ON C.Company = OH.Company AND C.CustNum = OH.CustNum
INNER JOIN Erp.Customer_UD CUD ON CUD.ForeignSysRowID = C.SysRowID
WHERE	OH.Company = @Company and OH.OrderNum > @OrderNum
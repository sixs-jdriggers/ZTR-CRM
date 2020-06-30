select	epic6s_accountc2echangeId as Idx,
		epic6s_FieldName as FieldName,
		epic6s_FieldValueString as FieldValue,
		epic6s_ChangedAccountIdEp as AccountId
from [dbo].[epic6s_accountc2echangeBase]
where epic6s_Processed = 0
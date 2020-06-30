SELECT	epic6s_contactc2eId as Idx,
		epic6s_FieldName as FieldName,
		epic6s_FieldValueString as FieldValue,
		epic6s_ReferenceID as ContactReference
FROM	[dbo].[epic6s_contactc2eBase]
WHERE	epic6s_Processed = 0 and epic6s_ReferenceID IS NOT NULL

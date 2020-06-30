SELECT	epic6s_termsId,
		epic6s_TermsCode as TermsCode,
		epic6s_terms as Description,
		ModifiedOn as ChangeDate
FROM	[dbo].[epic6s_termsBase]
WHERE	ModifiedOn >= '#MDATE#'
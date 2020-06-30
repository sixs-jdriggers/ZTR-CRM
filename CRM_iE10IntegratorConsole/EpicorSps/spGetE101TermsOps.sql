SELECT	Key1 as Idx,
		Key2 as TermsCode,
		Character01 as CRMID,
		Character02 as Description
FROM	Ice.UD01
WHERE	CheckBox01 = 0 and Key3 = 'E101TermsData'
SELECT	Key1 as Idx,
		Key2 as CustNum,
		Character01 as Address1,
		Character02 as Address2,
		Character03 as Address3,
		Character04 as State,
		Character05 as City,
		Character06 as Name,
		Character07 as Zip,
		Character08 as TermsCode,
		Character09 as GroupCode,
		Character10 as AccountID,
		ShortChar01 as CountryNum,
		ShortChar02 as SalesRepCode,
		CheckBox01 as status
FROM	Ice.UD01
WHERE	CheckBox01 = 0 and Key3 = 'E101'


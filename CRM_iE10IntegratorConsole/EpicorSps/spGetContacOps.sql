SELECT	Key1 as Idx,
		Key2 as PerConID,
		Character01 as PerConID2,
		Character02 as PhoneNum,
		Character03 as CellPhoneNum,
		Character04 as Name,
		Character05 as CRMAccountID,
		Character06 as CRMcontactID,
		Character07 as EmailAddress
FROM	Ice.UD01
WHERE	CheckBox01 = 0 and Key3 = 'E101CustContact3'


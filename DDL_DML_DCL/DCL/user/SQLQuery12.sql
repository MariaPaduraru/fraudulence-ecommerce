create login db_usertest --creare login
with password = 'parolatest1234';

--creare user: 
use fraud_ecommerce;
GO

create user usertest for login db_usertest;
--este creat dar nu am access la baza de date 

grant select on Processed.transactions to usertest

grant select on schema :: Processed to usertest

execute as login = 'db_usertest'

select * from Processed.transactions



revert;-- ne muta la contextul de dinainte(strict in sheetul respectiv), adica utilizator admin



execute as login = 'db_usertest'

execute as login = 'db_usertest'
revert

-- doar citire
alter role db_datareader add member db_usertest;

---- daca vrei si scriere
--alter role db_datawriter add member db_usertest;


use fraud_ecommerce;
go
create user db_usertest for login db_usertest;

drop user usertest

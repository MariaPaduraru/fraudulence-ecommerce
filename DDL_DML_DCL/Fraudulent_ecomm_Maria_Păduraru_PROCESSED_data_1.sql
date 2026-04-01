CREATE DATABASE fraud_ecommerce;
use fraud_ecommerce ;

create table Customers(
customer_id int primary key identity (1,1), 
age int, 
location varchar(255), 
account_age_days int NOT NULL
);

create table payment_methods (
    payment_method_id int primary key identity (1,1),
    method_name VARCHAR(50) NOT NULL UNIQUE
);

create table products (
    product_id int primary key identity (1,1),
    category VARCHAR(100) NOT NULL
);

create table devices (
    device_id int primary key identity (1,1),
    device_type VARCHAR(50) NOT NULL UNIQUE
);

create table transactions (
    transaction_id int primary key identity (1,1),
    customer_id int NOT NULL,
    product_id INT NOT NULL,
    payment_method_id INT NOT NULL,
    device_id INT NOT NULL,
    transaction_amount DECIMAL(10,2) NOT NULL,
    transaction_date DATETIME NOT NULL,
    quantity INT NOT NULL,
    ip_address VARCHAR(50),
    shipping_address TEXT,
    billing_address TEXT,
    is_fraudulent bit NOT NULL ,
    transaction_hour INT NOT NULL,

    foreign key (customer_id) references customers(customer_id),
    foreign key (product_id) references products(product_id),
    foreign key (payment_method_id) references payment_methods(payment_method_id),
    foreign key (device_id) references devices(device_id)
);

DROP TABLE transactions;
DROP TABLE customers;

CREATE TABLE customers (
    customer_id CHAR(36) PRIMARY KEY,
    age INT,
    location VARCHAR(255),
    account_age_days INT NOT NULL
);

CREATE TABLE transactions (
    transaction_id char(36) primary key,
    customer_id char(36) NOT NULL,
    product_id int NOT NULL,
    payment_method_id int NOT NULL,
    device_id int NOT NULL,
    transaction_amount decimal(10,2) NOT NULL,
    transaction_date datetime NOT NULL,
    quantity int NOT NULL,
    ip_address varchar(50),
    shipping_address TEXT,
    billing_address TEXT,
    is_fraudulent BIT NOT NULL,
    transaction_hour INT NOT NULL,

    foreign key (customer_id) references customers(customer_id),
    foreign key (product_id) references products(product_id),
    foreign key (payment_method_id) references payment_methods(payment_method_id),
    foreign key (device_id) references devices(device_id)
);


DROP TABLE transactions;--am scris gresit la shipping address si la billing address ca si text -- trb cu varchar(MAX)


create table transactions (
    transaction_id char(36) primary key, 
    customer_id char(36) NOT NULL,
    product_id int NOT NULL,
    payment_method_id int NOT NULL,
    device_id int NOT NULL,
    transaction_amount decimal(10,2) NOT NULL,
    transaction_date datetime NOT NULL,
    quantity int NOT NULL,
    ip_address varchar(50),
    shipping_address varchar(MAX),  -- TEXT este pt Oracle, în SQL Server se foloseste VARCHAR(MAX)
    billing_address varchar(MAX),
    is_fraudulent BIT NOT NULL DEFAULT 0,
    transaction_hour INT NOT NULL,

    foreign key (customer_id) references customers(customer_id),
    foreign key (product_id) references products(product_id),
    foreign key (payment_method_id) references payment_methods(payment_method_id),
    foreign key (device_id) references devices(device_id)
);

select * from customers

drop table transactions
drop table devices

create table Address(
address_id int primary key identity(1,1), 
shipping_address nvarchar(500), 
billing_address nvarchar(500));

create table devices(
device_id int primary key identity(1,1), 
device_type VARCHAR(50) NOT NULL UNIQUE, 
ip_address varchar(50) NOT NULL);

create table transactions(
transaction_id int primary key identity(1,1), 
customer_id char(36) NOT NULL, 
product_id int NOT NULL, 
payment_method_id int NOT NULL, 
ip_address varchar(50) NOT NULL, 
shipping_address nvarchar(500), 
billing_address nvarchar(500), 
transaction_amount decimal(10,2) NOT NULL,
transaction_date datetime NOT NULL,
quantity int NOT NULL, 
is_fraudulent BIT NOT NULL DEFAULT 0, 
transaction_hour INT NOT NULL, 

foreign key (customer_id) references Customers(customer_id),
foreign key (product_id) references products(product_id),
foreign key (payment_method_id) references payment_methods(payment_method_id), 
foreign key (ip_address) references devices(ip_address), 
foreign key (shipping_address) references Address (shipping_address), 
foreign key (billing_address) references Address(billing_address)
);
--nu pot face tabelul de transaction pentru ca ip_address nu e PK, cum fac ? 
--am declarat chei unice ip_address si billing address: 
ALTER TABLE devices
ADD CONSTRAINT UQ_devices_ip_address UNIQUE (ip_address);-- fac ip_address o CHEIE UNICA

-- Aceeasi problema am si la shipping_address si la billing address: 
ALTER TABLE Address
ADD CONSTRAINT UQ_Address_shipping_address UNIQUE (shipping_address);

ALTER TABLE Address
ADD CONSTRAINT UQ_Address_billing_address UNIQUE (billing_address);

---- a doua varianta era asa: In transactions table
--shipping_address_id INT,
--billing_address_id INT,

--FOREIGN KEY (shipping_address_id) REFERENCES Address(address_id),
--FOREIGN KEY (billing_address_id) REFERENCES Address(address_id)
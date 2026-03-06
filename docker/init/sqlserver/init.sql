-- Nautilus: dados de exemplo para teste read-only (SQL Server)
IF NOT EXISTS (SELECT * FROM sys.tables WHERE name = 'departments')
BEGIN
  CREATE TABLE departments (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(100) NOT NULL,
    created_at DATETIME2 DEFAULT GETDATE()
  );

  CREATE TABLE employees (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(200) NOT NULL,
    department_id INT,
    salary DECIMAL(12,2),
    hired_at DATE,
    created_at DATETIME2 DEFAULT GETDATE(),
    FOREIGN KEY (department_id) REFERENCES departments(id)
  );

  CREATE TABLE products (
    id INT IDENTITY(1,1) PRIMARY KEY,
    name NVARCHAR(200) NOT NULL,
    price DECIMAL(12,2) NOT NULL,
    created_at DATETIME2 DEFAULT GETDATE()
  );

  INSERT INTO departments (id, name) VALUES
    (1, 'Vendas'),
    (2, 'TI'),
    (3, 'RH');

  SET IDENTITY_INSERT employees ON;
  INSERT INTO employees (id, name, department_id, salary, hired_at) VALUES
    (1, 'Ana Silva', 1, 5500.00, '2022-03-15'),
    (2, 'Bruno Santos', 2, 7200.00, '2021-06-01'),
    (3, 'Carla Oliveira', 1, 4800.00, '2023-01-10'),
    (4, 'Diego Costa', 2, 6800.00, '2020-11-20'),
    (5, 'Elena Ferreira', 3, 5100.00, '2022-08-05');
  SET IDENTITY_INSERT employees OFF;

  INSERT INTO products (name, price) VALUES
    ('Notebook Pro', 4500.00),
    ('Mouse Wireless', 120.00),
    ('Teclado Mecânico', 380.00),
    ('Monitor 27"', 1200.00),
    ('Webcam HD', 250.00);
END
GO

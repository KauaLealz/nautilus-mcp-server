-- Nautilus: dados de exemplo para teste read-only (PostgreSQL)
CREATE TABLE IF NOT EXISTS departments (
  id SERIAL PRIMARY KEY,
  name VARCHAR(100) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS employees (
  id SERIAL PRIMARY KEY,
  name VARCHAR(200) NOT NULL,
  department_id INTEGER REFERENCES departments(id),
  salary DECIMAL(12,2),
  hired_at DATE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS products (
  id SERIAL PRIMARY KEY,
  name VARCHAR(200) NOT NULL,
  price DECIMAL(12,2) NOT NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

INSERT INTO departments (id, name) VALUES
  (1, 'Vendas'),
  (2, 'TI'),
  (3, 'RH')
ON CONFLICT (id) DO NOTHING;

INSERT INTO employees (id, name, department_id, salary, hired_at) VALUES
  (1, 'Ana Silva', 1, 5500.00, '2022-03-15'),
  (2, 'Bruno Santos', 2, 7200.00, '2021-06-01'),
  (3, 'Carla Oliveira', 1, 4800.00, '2023-01-10'),
  (4, 'Diego Costa', 2, 6800.00, '2020-11-20'),
  (5, 'Elena Ferreira', 3, 5100.00, '2022-08-05')
ON CONFLICT (id) DO NOTHING;

INSERT INTO products (id, name, price) VALUES
  (1, 'Notebook Pro', 4500.00),
  (2, 'Mouse Wireless', 120.00),
  (3, 'Teclado Mecânico', 380.00),
  (4, 'Monitor 27"', 1200.00),
  (5, 'Webcam HD', 250.00)
ON CONFLICT (id) DO NOTHING;

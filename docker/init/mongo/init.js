// Nautilus: dados de exemplo para teste read-only (MongoDB)
db = db.getSiblingDB("nautilus");

db.departments.drop();
db.departments.insertMany([
  { _id: 1, name: "Vendas", created_at: new Date() },
  { _id: 2, name: "TI", created_at: new Date() },
  { _id: 3, name: "RH", created_at: new Date() }
]);

db.employees.drop();
db.employees.insertMany([
  { _id: 1, name: "Ana Silva", department_id: 1, salary: 5500, hired_at: new Date("2022-03-15") },
  { _id: 2, name: "Bruno Santos", department_id: 2, salary: 7200, hired_at: new Date("2021-06-01") },
  { _id: 3, name: "Carla Oliveira", department_id: 1, salary: 4800, hired_at: new Date("2023-01-10") },
  { _id: 4, name: "Diego Costa", department_id: 2, salary: 6800, hired_at: new Date("2020-11-20") },
  { _id: 5, name: "Elena Ferreira", department_id: 3, salary: 5100, hired_at: new Date("2022-08-05") }
]);

db.products.drop();
db.products.insertMany([
  { _id: 1, name: "Notebook Pro", price: 4500 },
  { _id: 2, name: "Mouse Wireless", price: 120 },
  { _id: 3, name: "Teclado Mecânico", price: 380 },
  { _id: 4, name: "Monitor 27\"", price: 1200 },
  { _id: 5, name: "Webcam HD", price: 250 }
]);

print("MongoDB init: collections departments, employees, products criadas e populadas.");

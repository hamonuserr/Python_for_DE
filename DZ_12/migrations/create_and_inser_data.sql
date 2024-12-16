-- Create the client_info table
CREATE TABLE client_info (
    user_id INT PRIMARY KEY,
    name VARCHAR(100) NOT NULL,
    registration_address VARCHAR(255) NOT NULL,
    last_known_location VARCHAR(255) NOT NULL
);

-- Insert sample data into the client_info table
INSERT INTO client_info (user_id, name, registration_address, last_known_location) VALUES
(1, 'John Doe', 'New York', 'Los Angeles'),
(2, 'Jane Smith', 'Chicago', 'San Francisco'),
(3, 'Michael Johnson', 'Houston', 'Austin'),
(4, 'Emily Davis', 'Miami', 'Orlando'),
(5, 'Chris Brown', 'Boston', 'New York'),
(6, 'Anna White', 'Seattle', 'Portland'),
(7, 'David Miller', 'San Diego', 'Las Vegas'),
(8, 'Laura Wilson', 'Philadelphia', 'Washington DC'),
(9, 'Kevin Lee', 'Denver', 'Chicago'),
(10, 'Sophia Martinez', 'Phoenix', 'San Jose');


-- Создание таблицы transactions
CREATE TABLE transactions (
    transaction_id SERIAL PRIMARY KEY,
    user_id INT NOT NULL CHECK (user_id BETWEEN 1 AND 10),
    amount DECIMAL(10, 2) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    location VARCHAR(100) NOT NULL
);

-- Вставка 20 транзакций
INSERT INTO transactions (user_id, amount, timestamp, location) VALUES
(1, 1200.50, '2024-12-08T01:30:00', 'New York'),
(2, 150.75, '2024-12-08T02:15:00', 'Los Angeles'),
(3, 300.00, '2024-12-08T03:45:00', 'Chicago'),
(4, 2000.99, '2024-12-08T04:20:00', 'Houston'),
(5, 50.00, '2024-12-08T05:00:00', 'San Francisco'),
(6, 400.25, '2024-12-08T06:10:00', 'Seattle'),
(7, 1750.50, '2024-12-08T07:30:00', 'Boston'),
(8, 600.00, '2024-12-08T08:00:00', 'Miami'),
(9, 900.00, '2024-12-08T09:15:00', 'Philadelphia'),
(10, 50.99, '2024-12-08T10:20:00', 'Denver'),
(1, 275.50, '2024-12-08T11:45:00', 'Atlanta'),
(2, 325.75, '2024-12-08T12:30:00', 'Dallas'),
(3, 1025.00, '2024-12-08T13:15:00', 'Austin'),
(4, 700.25, '2024-12-08T14:30:00', 'San Diego'),
(5, 50.50, '2024-12-08T15:00:00', 'Portland'),
(6, 800.99, '2024-12-08T16:40:00', 'Las Vegas'),
(7, 1150.00, '2024-12-08T17:10:00', 'Washington DC'),
(8, 350.00, '2024-12-08T18:20:00', 'Phoenix'),
(9, 90.25, '2024-12-08T19:30:00', 'Detroit'),
(10, 500.75, '2024-12-08T20:45:00', 'Minneapolis');

-- Generate MySQL inventory CDC events after inventory-connector is running.

INSERT INTO inventorydb.inventory (product_name, quantity)
VALUES ('Widget D', 25);

UPDATE inventorydb.inventory
SET quantity = quantity - 5
WHERE id = 2;

DELETE FROM inventorydb.inventory
WHERE id = 3;


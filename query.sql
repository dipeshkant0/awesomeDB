SELECT o_orderkey, o_totalprice, '' 
FROM orders
WHERE o_totalprice > 100000.00;
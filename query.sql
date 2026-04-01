SELECT o_orderkey, o_totalprice, '' 
FROM orders
where o_totalprice > 100000.0;
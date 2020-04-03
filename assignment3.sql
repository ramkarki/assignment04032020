--3.	Write SQL to list all the salesman who did not make any sales in 2018

SELECT SALESMAN_NAME from	
(Select S.SALESMAN_NAME, SUM(COALESCE(O.ORDER_AMOUNT, 0)) AS TOTAL_SALES
From SALESMAN S
Left outer join ORDERS O
Where TO_CHAR ( SALES_DATE , 'YYYY') = '2018'
GROUP BY S.SALESMAN_NAME) 
WHERE  TOTAL_SALES = 0

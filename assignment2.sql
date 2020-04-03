--2.	Write SQL query to list Sales amount for each department in the year 2018 sorted by the sales amount (Show all the department even if it did not make any sales)

Select D.PEPARTMENT_NAME, Sum (O.ORDER_AMOUNT)  as TOTAL_SALES_AMT
From ORDERS O
Right outer join SALES_MAN S
On O.SALESMAN_ID = S. SALESMAN_ID
Inner join DEPARTMENT D
On S.DEPT_ID = D.DEPT_ID
Where TO_CHAR ( SALES_DATE , 'YYYY') = '2018'
Group by D.DEPARTMENT_NAME
Order by TOTAL_SALES_AMT

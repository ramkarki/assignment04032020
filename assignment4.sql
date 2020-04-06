--4.	Write SQL to list Top 10 Salesman in the Year 2018 based on the sales

SELECT SALESMAN_ID, SALESMAN_NAME, TOTAL_AMT FROM 
  	(Select SALESMAN_ID, SALESMAN_NAME , TOTAL_AMT,
    		Dense_rank () over ( Order by TOTAL_AMT Desc) RNK
    		(Select S.SALESMAN_ID, S.SALESMAN_NAME, 
                	Sum (O.ORDER_AMT) AS TOTAL_AMT
		From SALESMAN S
		Inner join ORDERS O
		On S.SALESMAN_ID = O.SALESMAN_ID = O.SALESMAN_ID
		Where TO_CHAR (O.SALES_DATE, 'YYYY') = '2018'
    		Group by S.SALESMAN_ID, S.SALESMAN_NAME 
   		)
   )
Where RNK <= 10

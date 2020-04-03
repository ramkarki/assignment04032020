-- 1.	Write SQL Query to list all the Sales for the state of California in the year 2018

Select C.*, O.* from Customer C
Inner Join Orders O
On C.CUST_ID = O.CUST_ID
Upper(STATE) = 'CALIFORNIA'
And TO_CHR (O.SALES_DATE, 'YYYY') = '2018'
